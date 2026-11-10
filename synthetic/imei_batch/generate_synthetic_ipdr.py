#!/usr/bin/env python3
"""Synthetic lab data for the Vodafone IMEI Impala batch.

The script builds the production databases and tables:

  ipdr_db.ipdr_cisco_oct_2026
  ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI

Column names and types match SHOW CREATE TABLE. Identifiers are synthetic.
The request-table IMEIs from the customer extract are not copied.

SDV GaussianCopula learns the joint distribution of session behavior
(duration, volumes, ports, RAT, APN, circle) and of request context
(circle, status, logging delay). Join keys, date partitions, and the
measured request funnel are then applied on top of that sample. A
multi-table parent/child model is not used: it would attach every IPDR
row to one of the 115 requested IMEIs and remove the selective join this
benchmark exists to tune.

Preserved from the production profile and attachments:

  request rows                         152
  distinct IMEI values                 115
  distinct req_id                      152
  IMEI multiplicity                    5 x5, 17 x2, 93 x1
  earliest from_date                   2026-07-01 00:00:00
  latest to_date                       2026-11-04 21:22:32
  null IMEI / null period / inverted   0 / 0 / 0
  requests overlapping 11-17 Oct       36
  seven-day IMEI matches               66,259
  seven-day join output                8,607
  partition row weights                SHOW PARTITIONS histogram
  fact layout                          date_partition identity, Snappy Parquet
  fact column stats                    not computed, matching NDV -1

Scaled down:

  ipdr_cisco_oct_2026 physical size    about 3 GiB (override with --target-gib)
  request table                        kept at 152 rows; scaling it by the
                                       fact-table ratio would delete the join

Default layout scatters imei_14 and brackets every row group, so min/max
statistics cannot skip a row group. That is the observed production
behavior. Generate a second copy with --layout imei-sorted to test the
clustered layout from the tuning report.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import warnings
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from sdv.metadata import Metadata
    from sdv.single_table import GaussianCopulaSynthesizer
    warnings.filterwarnings(
        "ignore",
        message="We strongly recommend saving the metadata",
    )
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "SDV is not installed. From synthetic/imei_batch run: "
        "python3 -m pip install -r requirements.txt\n"
        f"Import error: {exc}"
    )


QUERY_DAYS = tuple(date(2026, 10, day) for day in range(11, 18))
EARLIEST_FROM = pd.Timestamp("2026-07-01 00:00:00")
LATEST_TO = pd.Timestamp("2026-11-04 21:22:32")
WINDOW_START_HOURS = (1, 4, 7, 10, 13)
WINDOW_HOURS = 8
TARGET_HITS = 8607
TARGET_IMEI_MATCHES = 66259
IMEI_BASE = 30_000_000_000_000
IMEI_SPAN = 59_999_999_999_999
SENTINEL_LOW = "10000000000000"
SENTINEL_HIGH = "99999999999999"

# SHOW PARTITIONS row counts. The Iceberg day number is days since 1970-01-01
# (2026-10-11 = 20737). The null partition (351 rows) is omitted.
PRODUCTION_PARTITION_ROWS: dict[date, int] = {
    date(2026, 8, 11): 5978823425,
    date(2026, 8, 16): 7150061861,
    date(2026, 8, 24): 2095,
    date(2026, 9, 6): 2,
    date(2026, 9, 7): 15,
    date(2026, 9, 8): 35,
    date(2026, 9, 9): 24,
    date(2026, 9, 10): 7,
    date(2026, 9, 11): 1,
    date(2026, 9, 12): 305889534,
    date(2026, 9, 13): 5800692812,
    date(2026, 9, 14): 811161806,
    date(2026, 9, 15): 2010896815,
    date(2026, 9, 16): 4,
    date(2026, 9, 17): 97,
    date(2026, 9, 18): 581,
    date(2026, 9, 19): 680,
    date(2026, 9, 20): 1778,
    date(2026, 9, 21): 2898,
    date(2026, 9, 22): 12667,
    date(2026, 9, 23): 4469,
    date(2026, 9, 24): 4009,
    date(2026, 9, 25): 4580,
    date(2026, 9, 26): 9814,
    date(2026, 9, 27): 4804,
    date(2026, 9, 28): 9017,
    date(2026, 9, 29): 11533,
    date(2026, 9, 30): 47549547265,
    date(2026, 10, 1): 240902380676,
    date(2026, 10, 2): 238864750322,
    date(2026, 10, 3): 243434712349,
    date(2026, 10, 4): 232809098748,
    date(2026, 10, 5): 233627797733,
    date(2026, 10, 6): 235182401301,
    date(2026, 10, 7): 220720402480,
    date(2026, 10, 8): 171503284719,
    date(2026, 10, 9): 240519019467,
    date(2026, 10, 10): 235924747901,
    date(2026, 10, 11): 236625057219,
    date(2026, 10, 12): 235351519659,
    date(2026, 10, 13): 234384976192,
    date(2026, 10, 14): 243233083775,
    date(2026, 10, 15): 308979458804,
    date(2026, 10, 16): 243933680321,
    date(2026, 10, 17): 247713000154,
    date(2026, 10, 18): 234273421819,
    date(2026, 10, 19): 237481449422,
    date(2026, 10, 20): 233162011648,
    date(2026, 10, 21): 234578638308,
    date(2026, 10, 22): 235726874890,
    date(2026, 10, 23): 235562143215,
    date(2026, 10, 24): 236145779330,
    date(2026, 10, 25): 243592722549,
    date(2026, 10, 26): 259590829849,
    date(2026, 10, 27): 260055677741,
    date(2026, 10, 28): 257717383621,
    date(2026, 10, 29): 234524339898,
    date(2026, 10, 30): 246968362339,
    date(2026, 10, 31): 228830705340,
}

CIRCLES = (
    "AP", "ASM", "BIH", "DEL", "GUJ", "HAR", "HP", "JNK", "KAR", "KER",
    "KOL", "MUM", "MPC", "NE", "ODI", "PUN", "RAJ", "TN", "UPE", "UPW", "WB",
)
RATS = ("EUTRAN", "EUTRAN", "EUTRAN", "EUTRAN", "NR", "UTRAN", "GERAN")
PDP_TYPES = ("IPV4", "IPV4", "IPV4", "IPV4V6", "IPV6")
APNS = ("www", "ims", "internet", "viinternet")
APPS = ("WhatsApp", "YouTube", "Facebook", "Instagram", "Chrome", "PlayStore")
PORT_CLASSES = ("443", "443", "443", "443", "80", "80", "53", "5228", "853", "other")

FACT_SCHEMA = pa.schema(
    [
        ("imsi", pa.string()),
        ("imei", pa.string()),
        ("imei_14", pa.string()),
        ("msisdn", pa.string()),
        ("mac_address", pa.string()),
        ("source_ip", pa.string()),
        ("source_port", pa.string()),
        ("destination_ip", pa.string()),
        ("destination_port", pa.string()),
        ("translated_ip", pa.string()),
        ("translated_port", pa.string()),
        ("first_cell_id_name_location", pa.string()),
        ("session_start_date_time", pa.timestamp("us")),
        ("session_end_date_time", pa.timestamp("us")),
        ("duration_in_sec", pa.string()),
        ("data_volume_uplink", pa.string()),
        ("data_volume_downlink", pa.string()),
        ("pgw_ip_address", pa.string()),
        ("charging_id", pa.string()),
        ("access_point_name", pa.string()),
        ("pdp_address_ipv4", pa.string()),
        ("pdp_address_ipv6", pa.string()),
        ("cgi_id", pa.string()),
        ("rat", pa.string()),
        ("pdp_type", pa.string()),
        ("tethering", pa.string()),
        ("application_name", pa.string()),
        ("subs_circle", pa.string()),
        ("date_partition", pa.date32()),
    ]
)

REQUEST_SCHEMA = pa.schema(
    [
        ("req_id", pa.string()),
        ("rec_id", pa.string()),
        ("request_status", pa.string()),
        ("report_type", pa.string()),
        ("num_type", pa.string()),
        ("report_value", pa.string()),
        ("from_date", pa.timestamp("us")),
        ("to_date", pa.timestamp("us")),
        ("from_dt", pa.date32()),
        ("to_dt", pa.date32()),
        ("circle_id", pa.string()),
        ("log_date", pa.timestamp("us")),
        ("filename", pa.string()),
        ("ip_version", pa.string()),
        ("public_ip_version", pa.string()),
        ("public_port", pa.string()),
        ("private_ip", pa.string()),
        ("private_ip_version", pa.string()),
        ("private_port", pa.string()),
        ("mac_address", pa.string()),
        ("public_ip", pa.string()),
        ("destination_ip", pa.string()),
        ("destination_port", pa.string()),
        ("destination_ip_version", pa.string()),
        ("incident_number", pa.string()),
        ("refresh_timestamp", pa.timestamp("us")),
    ]
)


@dataclass
class OverlapWindow:
    catalog_id: int
    imei_14: str
    day: date
    win_start: pd.Timestamp
    win_end: pd.Timestamp
    n_hits: int
    n_misses: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "SDV로 ipdr_db.ipdr_cisco_oct_2026 와 "
            "ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI 합성 데이터를 생성합니다. "
            "DB, 테이블, 컬럼 이름은 운영과 같습니다."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "예시:\n"
            "  python3 generate_synthetic_ipdr.py --output-dir /data/imei_synth\n"
            "  python3 generate_synthetic_ipdr.py --output-dir /data/imei_synth \\\n"
            "      --target-gib 3 --layout scattered\n"
            "  python3 generate_synthetic_ipdr.py --output-dir /data/imei_imei_sorted \\\n"
            "      --target-gib 3 --layout imei-sorted --seed 20261011\n"
        ),
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--target-gib", type=float, default=3.0)
    parser.add_argument(
        "--target-mib",
        type=float,
        default=None,
        help="Overrides --target-gib. Useful for a smoke run.",
    )
    parser.add_argument("--seed", type=int, default=20261011)
    parser.add_argument(
        "--layout",
        choices=("scattered", "imei-sorted", "zorder"),
        default="scattered",
        help=(
            "scattered matches the profile (no imei_14 row-group skip). "
            "imei-sorted clusters imei_14. zorder sorts by the production "
            "ordering columns msisdn, cell, translated_ip."
        ),
    )
    parser.add_argument("--background-imei-ndv", type=int, default=2_000_000)
    parser.add_argument("--max-files-per-day", type=int, default=4)
    parser.add_argument("--row-group-mib", type=float, default=32.0)
    parser.add_argument("--seed-rows", type=int, default=8_000)
    parser.add_argument("--pool-rows", type=int, default=20_000)
    return parser.parse_args()


def fit_gaussian(frame: pd.DataFrame, seed: int, label: str, table_name: str):
    # SDV 2 expects one named table. infer_keys is off so a numeric column
    # is not mistaken for a primary key.
    metadata = Metadata.detect_from_dataframes({table_name: frame}, infer_keys=None)
    synthesizer = GaussianCopulaSynthesizer(metadata)
    np.random.seed(seed)
    print(f"Fitting SDV GaussianCopula on {label} ({len(frame):,} seed rows)")
    synthesizer.fit({table_name: frame})
    np.random.seed(seed)
    return synthesizer, metadata, table_name


def behavior_seed(rng: np.random.Generator, n: int) -> pd.DataFrame:
    port_class = rng.choice(PORT_CLASSES, size=n)
    # Volume and duration move together, and 443 sessions are heavier.
    # SDV learns this joint distribution from the seed.
    heavy = np.isin(port_class, ("443", "5228"))
    duration = rng.lognormal(np.where(heavy, 4.2, 3.4), 0.7, size=n)
    vol_down = rng.lognormal(np.where(heavy, 10.5, 8.2), 1.0, size=n)
    vol_up = vol_down * rng.uniform(0.02, 0.35, size=n)
    return pd.DataFrame(
        {
            "port_class": port_class,
            "source_port": rng.integers(1024, 65536, size=n),
            "translated_port": rng.integers(1024, 65536, size=n),
            "duration_sec": np.clip(np.rint(duration), 1, 86_400).astype(np.int64),
            "vol_up": np.clip(np.rint(vol_up), 0, 50_000_000_000).astype(np.int64),
            "vol_down": np.clip(np.rint(vol_down), 0, 50_000_000_000).astype(np.int64),
            "second_of_day": rng.integers(0, 86_400, size=n),
            "rat": rng.choice(RATS, size=n),
            "pdp_type": rng.choice(PDP_TYPES, size=n),
            "apn": rng.choice(APNS, size=n),
            "tethering": rng.choice(("0", "0", "0", "0", "1"), size=n),
            "application_name": rng.choice(APPS, size=n),
            "subs_circle": rng.choice(CIRCLES, size=n),
        }
    )


def request_context_seed(rng: np.random.Generator, n: int) -> pd.DataFrame:
    status = rng.choice(("COMPLETED", "COMPLETED", "COMPLETED", "PARTIAL"), size=n)
    delay = rng.integers(5, 180, size=n) + np.where(status == "PARTIAL", 240, 0)
    return pd.DataFrame(
        {
            "circle_id": rng.choice(CIRCLES, size=n),
            "request_status": status,
            "refresh_delay_min": delay.astype(np.int64),
        }
    )


def clean_behavior(frame: pd.DataFrame) -> dict[str, np.ndarray]:
    port_class = frame["port_class"].astype(str).to_numpy()
    known = {"443", "80", "53", "5228", "853", "other"}
    port_class = np.where(np.isin(port_class, list(known)), port_class, "other")
    return {
        "port_class": port_class,
        "source_port": _clip_int(frame["source_port"], 1, 65535),
        "translated_port": _clip_int(frame["translated_port"], 1, 65535),
        "duration_sec": _clip_int(frame["duration_sec"], 1, 86_400),
        "vol_up": _clip_int(frame["vol_up"], 0, 50_000_000_000),
        "vol_down": _clip_int(frame["vol_down"], 0, 50_000_000_000),
        "second_of_day": _clip_int(frame["second_of_day"], 0, 86_399),
        "rat": _clean_cat(frame["rat"], RATS),
        "pdp_type": _clean_cat(frame["pdp_type"], PDP_TYPES),
        "apn": _clean_cat(frame["apn"], APNS),
        "tethering": _clean_cat(frame["tethering"], ("0", "1")),
        "application_name": _clean_cat(frame["application_name"], APPS),
        "subs_circle": _clean_cat(frame["subs_circle"], CIRCLES),
    }


def _clip_int(series: pd.Series, low: int, high: int) -> np.ndarray:
    values = pd.to_numeric(series, errors="coerce").fillna((low + high) / 2)
    return np.clip(np.rint(values.to_numpy()), low, high).astype(np.int64)


def _clean_cat(series: pd.Series, allowed) -> np.ndarray:
    allowed_list = [str(value) for value in dict.fromkeys(allowed)]
    values = series.astype(str).to_numpy()
    return np.where(np.isin(values, allowed_list), values, allowed_list[0])


def print_sdv_check(seed: pd.DataFrame, sample: pd.DataFrame) -> None:
    seed_mean = float(seed["duration_sec"].mean())
    sample_mean = float(pd.to_numeric(sample["duration_sec"]).mean())
    seed_share = float((seed["port_class"] == "443").mean())
    sample_share = float((sample["port_class"].astype(str) == "443").mean())
    print(
        "SDV behavior check: "
        f"duration mean {seed_mean:.1f} -> {sample_mean:.1f}, "
        f"port 443 share {seed_share:.3f} -> {sample_share:.3f}"
    )


def luhn_check_digits(imei14: np.ndarray) -> np.ndarray:
    text = np.ascontiguousarray(np.asarray(imei14, dtype="U14"))
    # NumPy stores UCS-4 code units as uint32. astype(int) would parse the
    # character as a number, so read the code point and subtract ASCII '0'.
    codes = text.view(np.uint32).reshape(text.shape[0], -1)[:, :14]
    digits = codes.astype(np.int16) - ord("0")
    if digits.shape[1] != 14 or (digits < 0).any() or (digits > 9).any():
        raise RuntimeError("IMEI-14 values must be 14 digits before the Luhn step")
    total = np.zeros(len(text), dtype=np.int32)
    for index, column in enumerate(range(13, -1, -1)):
        value = digits[:, column].astype(np.int32)
        if index % 2 == 0:
            value *= 2
            value = np.where(value > 9, value - 9, value)
        total += value
    return ((10 - (total % 10)) % 10).astype(np.int64)


def format_imei14(catalog_ids: np.ndarray, stride: int) -> np.ndarray:
    values = IMEI_BASE + np.asarray(catalog_ids, dtype=np.int64) * np.int64(stride)
    if values.min() < IMEI_BASE or values.max() > IMEI_BASE + IMEI_SPAN:
        raise RuntimeError("IMEI catalog overflowed the 14-digit synthetic range")
    return np.char.zfill(values.astype(str), 14)


def format_imei15(imei14: np.ndarray) -> np.ndarray:
    check = luhn_check_digits(imei14).astype(str)
    return np.char.add(np.asarray(imei14, dtype="U14"), check.astype("U1"))


def format_imsi(catalog_ids: np.ndarray) -> np.ndarray:
    values = 404_670_000_000_000 + np.asarray(catalog_ids, dtype=np.int64)
    return np.char.zfill(values.astype(str), 15)


def format_msisdn(catalog_ids: np.ndarray) -> np.ndarray:
    values = 6_000_000_000 + np.asarray(catalog_ids, dtype=np.int64)
    return values.astype(str)


def dotted(a, b, c, d) -> np.ndarray:
    left = np.char.add(np.char.add(a.astype(str), "."), np.char.add(b.astype(str), "."))
    right = np.char.add(np.char.add(c.astype(str), "."), d.astype(str))
    return np.char.add(left, right)


def take_behavior(pool: dict[str, np.ndarray], n: int, rng: np.random.Generator):
    index = rng.integers(0, len(pool["duration_sec"]), size=n)
    taken = {name: values[index] for name, values in pool.items()}
    dest = np.empty(n, dtype=np.int64)
    for label, port in (("443", 443), ("80", 80), ("53", 53), ("5228", 5228), ("853", 853)):
        mask = taken["port_class"] == label
        dest[mask] = port
    other = taken["port_class"] == "other"
    dest[other] = rng.integers(1024, 65536, size=int(other.sum()))
    taken["destination_port"] = dest
    return taken


def stamp(day: date, hour: int, minute: int = 0, second: int = 0) -> pd.Timestamp:
    return pd.Timestamp(
        year=day.year, month=day.month, day=day.day, hour=hour, minute=minute, second=second
    )


def outside_periods() -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    return [
        (pd.Timestamp("2026-09-01 00:00:00"), pd.Timestamp("2026-09-30 23:59:59")),
        (pd.Timestamp("2026-10-01 00:00:00"), pd.Timestamp("2026-10-10 23:59:59")),
        (pd.Timestamp("2026-10-18 00:00:00"), pd.Timestamp("2026-10-31 23:59:59")),
        (pd.Timestamp("2026-11-01 00:00:00"), LATEST_TO),
        (pd.Timestamp("2026-09-22 18:00:00"), pd.Timestamp("2026-09-22 21:05:00")),
        (pd.Timestamp("2026-10-03 00:00:00"), pd.Timestamp("2026-10-05 23:59:00")),
        (pd.Timestamp("2026-08-20 00:00:00"), pd.Timestamp("2026-08-27 23:59:59")),
    ]


def hot_outside_periods() -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    return [
        (EARLIEST_FROM, pd.Timestamp("2026-08-31 23:59:59")),
        (pd.Timestamp("2026-09-01 00:00:00"), pd.Timestamp("2026-09-30 23:59:59")),
        (pd.Timestamp("2026-10-01 00:00:00"), pd.Timestamp("2026-10-10 23:59:59")),
        (pd.Timestamp("2026-10-18 00:00:00"), LATEST_TO),
    ]


def build_requests(
    context: pd.DataFrame,
    request_ids: np.ndarray,
    stride: int,
) -> tuple[pd.DataFrame, list[OverlapWindow]]:
    """152 requests. 36 of them overlap 11-17 Oct, on partial-day windows."""
    if len(request_ids) != 115:
        raise RuntimeError("Expected 115 request catalog ids")
    periods: list[tuple[int, pd.Timestamp, pd.Timestamp, OverlapWindow | None]] = []
    overlaps: list[OverlapWindow] = []

    def add_overlap(slot: int, catalog_id: int) -> None:
        day = QUERY_DAYS[slot % 7]
        start_hour = WINDOW_START_HOURS[slot % len(WINDOW_START_HOURS)]
        win_start = stamp(day, start_hour)
        win_end = stamp(day, start_hour + WINDOW_HOURS)
        n_hits = 240 if slot < 3 else 239
        # 16 x 1602 + 20 x 1601 = 57652; with 8607 hits this is 66259.
        n_misses = 1602 if slot < 16 else 1601
        window = OverlapWindow(
            catalog_id=int(catalog_id),
            imei_14=str(format_imei14(np.array([catalog_id]), stride)[0]),
            day=day,
            win_start=win_start,
            win_end=win_end,
            n_hits=n_hits,
            n_misses=n_misses,
        )
        overlaps.append(window)
        periods.append((int(catalog_id), win_start, win_end, window))

    # 5 IMEIs x 5 requests. One segment each overlaps the query week.
    for offset in range(5):
        catalog_id = int(request_ids[offset])
        outside = hot_outside_periods()
        for start, end in outside[:3]:
            periods.append((catalog_id, start, end, None))
        add_overlap(offset, catalog_id)
        periods.append((catalog_id, *outside[3], None))

    # 17 IMEIs x 2 requests, both outside the query week.
    pairs = (
        (0, 2),
        (1, 3),
        (0, 3),
        (4, 2),
        (5, 1),
    )
    for index in range(17):
        catalog_id = int(request_ids[5 + index])
        left, right = pairs[index % len(pairs)]
        choices = outside_periods()
        periods.append((catalog_id, *choices[left], None))
        periods.append((catalog_id, *choices[right], None))

    # 31 single-request IMEIs overlap the query week. 62 stay outside.
    for index in range(31):
        add_overlap(5 + index, int(request_ids[22 + index]))
    choices = outside_periods()
    for index in range(62):
        catalog_id = int(request_ids[53 + index])
        start, end = choices[index % len(choices)]
        periods.append((catalog_id, start, end, None))

    if len(periods) != 152 or len(overlaps) != 36:
        raise RuntimeError(
            f"Request construction produced {len(periods)} rows and {len(overlaps)} overlaps"
        )
    if sum(window.n_hits for window in overlaps) != TARGET_HITS:
        raise RuntimeError("Planted hit quota does not sum to 8607")
    if sum(window.n_hits + window.n_misses for window in overlaps) != TARGET_IMEI_MATCHES:
        raise RuntimeError("Planted IMEI-match quota does not sum to 66259")

    catalog_ids = np.array([item[0] for item in periods], dtype=np.int64)
    imei14 = format_imei14(catalog_ids, stride)
    ctx = context.reset_index(drop=True)
    if len(ctx) != 152:
        raise RuntimeError("Request context sample must contain 152 rows")
    from_date = pd.Series(pd.to_datetime([item[1] for item in periods]))
    to_date = pd.Series(pd.to_datetime([item[2] for item in periods]))
    delay = pd.to_numeric(ctx["refresh_delay_min"], errors="coerce").fillna(30).clip(1, 10_000)
    log_date = from_date - pd.to_timedelta(delay.to_numpy(), unit="m")
    circle = ctx["circle_id"].astype(str).to_numpy()
    req_id = np.array([f"R2026{index:05d}" for index in range(152)])
    incident = np.array([f"INC2026{(catalog // 4):04d}" for catalog in catalog_ids])
    filename = np.array(
        [
            f"lgl_imei_{circle[index]}_{from_date[index]:%Y%m%d}.csv"
            for index in range(152)
        ]
    )
    empty = pd.Series([None] * 152, dtype=object)
    frame = pd.DataFrame(
        {
            "req_id": req_id,
            "rec_id": np.char.add(req_id.astype(str), "-1"),
            "request_status": ctx["request_status"].astype(str).to_numpy(),
            "report_type": "IMEI",
            "num_type": "IMEI",
            "report_value": imei14,
            "from_date": from_date,
            "to_date": to_date,
            "from_dt": from_date.dt.date,
            "to_dt": to_date.dt.date,
            "circle_id": circle,
            "log_date": log_date,
            "filename": filename,
            "ip_version": empty,
            "public_ip_version": empty,
            "public_port": empty,
            "private_ip": empty,
            "private_ip_version": empty,
            "private_port": empty,
            "mac_address": empty,
            "public_ip": empty,
            "destination_ip": empty,
            "destination_port": empty,
            "destination_ip_version": empty,
            "incident_number": incident,
            "refresh_timestamp": log_date,
        }
    )
    _assert_request_invariants(frame)
    return frame, overlaps


def _assert_request_invariants(frame: pd.DataFrame) -> None:
    overlap = (frame["from_date"] < pd.Timestamp("2026-10-18")) & (
        frame["to_date"] >= pd.Timestamp("2026-10-11")
    )
    counts = frame.groupby("report_value").size()
    checks = {
        "rows": len(frame) == 152,
        "distinct_imei": frame["report_value"].nunique() == 115,
        "distinct_req": frame["req_id"].nunique() == 152,
        "num_type": set(frame["num_type"]) == {"IMEI"},
        "nulls": int(frame[["report_value", "from_date", "to_date"]].isna().sum().sum()) == 0,
        "order": bool((frame["from_date"] <= frame["to_date"]).all()),
        "overlap": int(overlap.sum()) == 36,
        "earliest": frame["from_date"].min() == EARLIEST_FROM,
        "latest": frame["to_date"].max() == LATEST_TO,
        "multiplicity": (
            int((counts == 5).sum()) == 5
            and int((counts == 2).sum()) == 17
            and int((counts == 1).sum()) == 93
        ),
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise RuntimeError(f"Request invariants failed: {', '.join(failed)}")


def _uniform_starts(
    n: int,
    low: pd.Timestamp,
    high: pd.Timestamp,
    rng: np.random.Generator,
) -> np.ndarray:
    span = int((high - low).total_seconds())
    if n == 0:
        return np.empty(0, dtype="datetime64[us]")
    if span <= 0:
        raise RuntimeError(f"Empty timestamp span {low} .. {high}")
    offset = rng.integers(0, span, size=n)
    start = np.datetime64(low.to_datetime64(), "s") + offset.astype("timedelta64[s]")
    return start.astype("datetime64[us]")


def build_planted(
    windows: list[OverlapWindow],
    pool: dict[str, np.ndarray],
    stride: int,
    rng: np.random.Generator,
) -> dict[date, dict[str, np.ndarray]]:
    grouped: dict[date, list[dict[str, np.ndarray]]] = {day: [] for day in QUERY_DAYS}
    for window in windows:
        behavior = take_behavior(pool, window.n_hits + window.n_misses, rng)
        starts, ends, durations = _window_sessions(window, rng)
        catalog = np.full(window.n_hits + window.n_misses, window.catalog_id, dtype=np.int64)
        columns = _assemble(
            catalog=catalog,
            behavior=behavior,
            starts=starts,
            ends=ends,
            durations=durations,
            day=window.day,
            stride=stride,
            rng=rng,
            spill=False,
        )
        grouped[window.day].append(columns)
    return {day: _concat_many(parts) for day, parts in grouped.items() if parts}


def _window_sessions(
    window: OverlapWindow, rng: np.random.Generator
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n_cross = window.n_hits // 7
    n_inside = window.n_hits - 2 * n_cross
    n_miss_before = window.n_misses // 2
    n_miss_after = window.n_misses - n_miss_before
    day_start = stamp(window.day, 0, 5, 0)
    before_end = window.win_start - pd.Timedelta(minutes=2)
    after_start = window.win_end + pd.Timedelta(minutes=2)
    day_late = stamp(window.day, 23, 50, 0)

    pieces = []
    pieces.append(
        _bounded_sessions(
            n_inside,
            window.win_start,
            window.win_end - pd.Timedelta(seconds=90),
            60,
            rng,
        )
    )
    pre = rng.integers(60, 20 * 60, size=n_cross)
    into = rng.integers(30, 10 * 60, size=n_cross)
    cross_start = window.win_start - pd.to_timedelta(pre, unit="s")
    cross_start_end = window.win_start + pd.to_timedelta(into, unit="s")
    pieces.append(_from_bounds(cross_start, cross_start_end))
    back = rng.integers(30, 10 * 60, size=n_cross)
    past = rng.integers(60, 20 * 60, size=n_cross)
    cross_end_start = window.win_end - pd.to_timedelta(back, unit="s")
    cross_end = window.win_end + pd.to_timedelta(past, unit="s")
    pieces.append(_from_bounds(cross_end_start, cross_end))
    pieces.append(_bounded_sessions(n_miss_before, day_start, before_end, 60, rng))
    pieces.append(_bounded_sessions(n_miss_after, after_start, day_late, 60, rng))
    starts = np.concatenate([item[0] for item in pieces])
    ends = np.concatenate([item[1] for item in pieces])
    durations = np.concatenate([item[2] for item in pieces])
    return starts, ends, durations


def _bounded_sessions(n, low, high, duration, rng):
    if n == 0:
        empty = np.empty(0, dtype="datetime64[us]")
        return empty, empty, np.empty(0, dtype=np.int64)
    start = _uniform_starts(n, low, high - pd.Timedelta(seconds=duration), rng)
    end = start + np.timedelta64(duration, "s")
    return start, end.astype("datetime64[us]"), np.full(n, duration, dtype=np.int64)


def _from_bounds(start: pd.Series, end: pd.Series):
    start_ns = start.to_numpy(dtype="datetime64[ns]").astype("datetime64[us]")
    end_ns = end.to_numpy(dtype="datetime64[ns]").astype("datetime64[us]")
    duration = ((end_ns - start_ns) / np.timedelta64(1, "s")).astype(np.int64)
    if (duration <= 0).any():
        raise RuntimeError("Session duration must be positive")
    return start_ns, end_ns, duration


def _assemble(
    catalog: np.ndarray,
    behavior: dict[str, np.ndarray],
    starts: np.ndarray,
    ends: np.ndarray,
    durations: np.ndarray,
    day: date,
    stride: int,
    rng: np.random.Generator,
    spill: bool,
) -> dict[str, np.ndarray]:
    n = len(catalog)
    imei14 = format_imei14(catalog, stride)
    if spill:
        spill_mask = rng.random(n) < 0.02
        if spill_mask.any():
            previous = np.datetime64(f"{(day - timedelta(days=1)).isoformat()}T22:00:00", "s")
            offset = (behavior["second_of_day"][spill_mask] % 7200).astype("timedelta64[s]")
            starts = starts.copy()
            ends = ends.copy()
            starts[spill_mask] = (previous + offset).astype("datetime64[us]")
            ends[spill_mask] = starts[spill_mask] + durations[spill_mask].astype("timedelta64[s]")
    source_b = np.full(n, 100, dtype=np.int64)
    source_c = 64 + (catalog % 64)
    source_d = (catalog // 64) % 256
    source_e = (catalog // 16_384) % 254 + 1
    trans_b = catalog % 256
    trans_c = (catalog // 256) % 256
    trans_d = (catalog // 65_536) % 254 + 1
    site = catalog % 20_000
    lac = 10_000 + (catalog % 5_000)
    sector = (catalog % 3) + 1
    circle = behavior["subs_circle"].astype(str)
    cell = np.char.add(
        np.char.add(circle.astype("U8"), "-SITE-"),
        np.char.add(np.char.zfill(site.astype(str), 5), np.char.add("-S", sector.astype(str))),
    )
    cgi = np.char.add(
        "404-67-",
        np.char.add(np.char.add(lac.astype(str), "-"), (site + 1).astype(str)),
    )
    pdp_v6 = np.empty(n, dtype=object)
    pdp_v6[:] = None
    ipv6_mask = (behavior["pdp_type"] == "IPV6") | (behavior["pdp_type"] == "IPV4V6")
    # Keep a majority of IPv6 slots empty; dual-stack rows still carry an address.
    ipv6_mask = ipv6_mask & (rng.random(n) < 0.35)
    if ipv6_mask.any():
        pdp_v6[ipv6_mask] = np.char.add("2001:db8:10:", np.char.mod("%x", catalog[ipv6_mask]))
    mac = _macs(n, rng)
    app = behavior["application_name"].astype(object)
    app[rng.random(n) < 0.50] = None
    cell_obj = cell.astype(object)
    cell_obj[rng.random(n) < 0.01] = None
    return {
        "imsi": format_imsi(catalog),
        "imei": format_imei15(imei14),
        "imei_14": imei14,
        "msisdn": format_msisdn(catalog),
        "mac_address": mac,
        "source_ip": dotted(source_b, source_c, source_d, source_e),
        "source_port": behavior["source_port"].astype(str),
        "destination_ip": dotted(
            np.full(n, 203),
            np.full(n, 0),
            np.full(n, 113),
            (catalog % 200) + 1,
        ),
        "destination_port": behavior["destination_port"].astype(str),
        "translated_ip": dotted(np.full(n, 10), trans_b, trans_c, trans_d),
        "translated_port": behavior["translated_port"].astype(str),
        "first_cell_id_name_location": cell_obj,
        "session_start_date_time": starts.astype("datetime64[us]"),
        "session_end_date_time": ends.astype("datetime64[us]"),
        "duration_in_sec": durations.astype(str),
        "data_volume_uplink": behavior["vol_up"].astype(str),
        "data_volume_downlink": behavior["vol_down"].astype(str),
        "pgw_ip_address": np.char.add("198.51.100.", ((catalog % 32) + 1).astype(str)),
        "charging_id": rng.integers(100_000_000_000, 1_000_000_000_000, size=n).astype(str),
        "access_point_name": behavior["apn"].astype(str),
        "pdp_address_ipv4": dotted(source_b, source_c, source_d, source_e),
        "pdp_address_ipv6": pdp_v6,
        "cgi_id": cgi,
        "rat": behavior["rat"].astype(str),
        "pdp_type": behavior["pdp_type"].astype(str),
        "tethering": behavior["tethering"].astype(str),
        "application_name": app,
        "subs_circle": circle.astype(str),
        "date_partition": np.full(n, np.datetime64(day, "D")),
    }


def _macs(n: int, rng: np.random.Generator) -> np.ndarray:
    raw = rng.integers(0, 256, size=(n, 6), dtype=np.uint8)
    raw[:, 0] = (raw[:, 0] | 0x02) & 0xFE
    text = np.char.mod("%02x", raw[:, 0])
    for column in range(1, 6):
        text = np.char.add(np.char.add(text, ":"), np.char.mod("%02x", raw[:, column]))
    values = text.astype(object)
    values[rng.random(n) < 0.70] = None
    return values


def _concat_many(parts: list[dict[str, np.ndarray]]) -> dict[str, np.ndarray]:
    keys = parts[0].keys()
    return {key: np.concatenate([part[key] for part in parts]) for key in keys}


def background_rows(
    n: int,
    day: date,
    pool: dict[str, np.ndarray],
    ndv: int,
    reserved: np.ndarray,
    stride: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    if n <= 0:
        return {}
    catalog = rng.integers(0, ndv, size=n, dtype=np.int64)
    bad = np.isin(catalog, reserved)
    while bad.any():
        catalog[bad] = rng.integers(0, ndv, size=int(bad.sum()), dtype=np.int64)
        bad = np.isin(catalog, reserved)
    behavior = take_behavior(pool, n, rng)
    base = np.datetime64(f"{day.isoformat()}T00:00:00", "s")
    starts = base + behavior["second_of_day"].astype("timedelta64[s]")
    durations = behavior["duration_sec"]
    ends = starts + durations.astype("timedelta64[s]")
    return _assemble(
        catalog=catalog,
        behavior=behavior,
        starts=starts.astype("datetime64[us]"),
        ends=ends.astype("datetime64[us]"),
        durations=durations,
        day=day,
        stride=stride,
        rng=rng,
        spill=True,
    )


def allocate_rows(
    target_rows: int,
    planted_per_day: dict[date, int],
) -> dict[date, int]:
    dates = list(PRODUCTION_PARTITION_ROWS)
    weights = np.array([PRODUCTION_PARTITION_ROWS[day] for day in dates], dtype=np.float64)
    weights /= weights.sum()
    raw = np.maximum(np.floor(weights * target_rows).astype(np.int64), 1)
    for index, day in enumerate(dates):
        raw[index] = max(int(raw[index]), int(planted_per_day.get(day, 0)))
    extra = int(raw.sum()) - int(target_rows)
    if extra > 0:
        for index in np.argsort(-raw):
            floor = max(1, int(planted_per_day.get(dates[index], 0)))
            take = min(extra, int(raw[index]) - floor)
            if take > 0:
                raw[index] -= take
                extra -= take
            if extra == 0:
                break
    elif extra < 0:
        raw[int(np.argmax(weights))] += -extra
    return {day: int(count) for day, count in zip(dates, raw)}


def write_table(path: Path, columns: dict[str, np.ndarray], schema: pa.Schema, row_group_rows: int):
    arrays = []
    for field in schema:
        values = columns[field.name]
        if pa.types.is_timestamp(field.type):
            arrays.append(pa.array(np.asarray(values, dtype="datetime64[us]"), type=field.type))
        elif pa.types.is_date(field.type):
            arrays.append(pa.array(np.asarray(values, dtype="datetime64[D]"), type=field.type))
        else:
            arrays.append(pa.array(values, type=field.type, from_pandas=True))
    table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(
        table,
        path,
        compression="snappy",
        row_group_size=max(1, row_group_rows),
        use_deprecated_int96_timestamps=True,
    )


def write_requests(frame: pd.DataFrame, path: Path) -> None:
    columns = {
        "req_id": frame["req_id"].to_numpy(),
        "rec_id": frame["rec_id"].to_numpy(),
        "request_status": frame["request_status"].to_numpy(),
        "report_type": frame["report_type"].to_numpy(),
        "num_type": frame["num_type"].to_numpy(),
        "report_value": frame["report_value"].to_numpy(),
        "from_date": frame["from_date"].to_numpy(dtype="datetime64[us]"),
        "to_date": frame["to_date"].to_numpy(dtype="datetime64[us]"),
        "from_dt": pd.to_datetime(frame["from_dt"]).to_numpy(dtype="datetime64[D]"),
        "to_dt": pd.to_datetime(frame["to_dt"]).to_numpy(dtype="datetime64[D]"),
        "circle_id": frame["circle_id"].to_numpy(),
        "log_date": frame["log_date"].to_numpy(dtype="datetime64[us]"),
        "filename": frame["filename"].to_numpy(),
        "ip_version": frame["ip_version"].to_numpy(),
        "public_ip_version": frame["public_ip_version"].to_numpy(),
        "public_port": frame["public_port"].to_numpy(),
        "private_ip": frame["private_ip"].to_numpy(),
        "private_ip_version": frame["private_ip_version"].to_numpy(),
        "private_port": frame["private_port"].to_numpy(),
        "mac_address": frame["mac_address"].to_numpy(),
        "public_ip": frame["public_ip"].to_numpy(),
        "destination_ip": frame["destination_ip"].to_numpy(),
        "destination_port": frame["destination_port"].to_numpy(),
        "destination_ip_version": frame["destination_ip_version"].to_numpy(),
        "incident_number": frame["incident_number"].to_numpy(),
        "refresh_timestamp": frame["refresh_timestamp"].to_numpy(dtype="datetime64[us]"),
    }
    write_table(path, columns, REQUEST_SCHEMA, row_group_rows=10_000)


def validate_join(planted: dict[date, dict[str, np.ndarray]], requests: pd.DataFrame) -> dict:
    frames = []
    for day, columns in planted.items():
        frames.append(
            pd.DataFrame(
                {
                    "imei_14": columns["imei_14"].astype(str),
                    "session_start_date_time": pd.to_datetime(columns["session_start_date_time"]),
                    "session_end_date_time": pd.to_datetime(columns["session_end_date_time"]),
                    "date_partition": day,
                }
            )
        )
    fact = pd.concat(frames, ignore_index=True)
    merged = fact.merge(
        requests,
        left_on="imei_14",
        right_on="report_value",
        how="inner",
    )
    start_ok = (merged["session_start_date_time"] >= merged["from_date"]) & (
        merged["session_start_date_time"] <= merged["to_date"]
    )
    end_ok = (merged["session_end_date_time"] >= merged["from_date"]) & (
        merged["session_end_date_time"] <= merged["to_date"]
    )
    matched = merged[start_ok | end_ok]
    by_day = {str(day): int(count) for day, count in matched.groupby("date_partition").size().items()}
    if len(fact) != TARGET_IMEI_MATCHES:
        raise RuntimeError(f"Planted IMEI rows are {len(fact)}, expected {TARGET_IMEI_MATCHES}")
    if len(matched) != TARGET_HITS:
        raise RuntimeError(f"Join output is {len(matched)}, expected {TARGET_HITS}")
    missing_days = [str(day) for day in QUERY_DAYS if str(day) not in by_day]
    if missing_days:
        raise RuntimeError(f"Query days with no matches: {', '.join(missing_days)}")
    return {"output_rows": int(len(matched)), "output_rows_by_date": by_day}


def permute(columns: dict[str, np.ndarray], perm: np.ndarray) -> dict[str, np.ndarray]:
    return {key: values[perm] for key, values in columns.items()}


def inject_sentinels_masked(
    columns: dict[str, np.ndarray],
    planted_mask: np.ndarray,
    row_group_rows: int,
) -> int:
    n = len(planted_mask)
    low15 = format_imei15(np.array([SENTINEL_LOW]))[0]
    high15 = format_imei15(np.array([SENTINEL_HIGH]))[0]
    injected = 0
    for start in range(0, n, row_group_rows):
        stop = min(start + row_group_rows, n)
        candidates = np.flatnonzero(~planted_mask[start:stop]) + start
        if len(candidates) < 2:
            raise RuntimeError(
                "A query-day row group has fewer than two background rows, so IMEI "
                "min/max cannot be forced wide. Increase --target-gib."
            )
        for index, imei14, imei15, ident in (
            (int(candidates[0]), SENTINEL_LOW, low15, 900_000_000),
            (int(candidates[1]), SENTINEL_HIGH, high15, 900_000_001),
        ):
            columns["imei_14"][index] = imei14
            columns["imei"][index] = imei15
            columns["imsi"][index] = f"{404670000000000 + ident}"
            columns["msisdn"][index] = str(6_000_000_000 + ident)
            injected += 1
    return injected


def files_for(n_rows: int, max_files: int) -> int:
    if n_rows < 80_000:
        return 1
    return max(1, min(max_files, int(np.ceil(n_rows / 250_000))))


def order_columns(
    columns: dict[str, np.ndarray],
    layout: str,
    rng: np.random.Generator,
) -> tuple[dict[str, np.ndarray], np.ndarray]:
    n = len(columns["imei_14"])
    if layout == "scattered":
        perm = rng.permutation(n)
    elif layout == "imei-sorted":
        perm = np.argsort(columns["imei_14"].astype(str), kind="mergesort")
    elif layout == "zorder":
        perm = np.argsort(_zorder(columns), kind="mergesort")
    else:
        raise RuntimeError(layout)
    return permute(columns, perm), perm


def _zorder(columns: dict[str, np.ndarray]) -> np.ndarray:
    parts = [
        _ten_bits(columns["msisdn"]),
        _ten_bits(columns["first_cell_id_name_location"].astype(str)),
        _ten_bits(columns["translated_ip"]),
    ]
    mixed = np.zeros(len(parts[0]), dtype=np.int64)
    for bit in range(10):
        for shift, part in enumerate(parts):
            mixed |= ((part >> bit) & 1) << (3 * bit + shift)
    return mixed


def _ten_bits(values: np.ndarray) -> np.ndarray:
    hashed = pd.util.hash_array(np.asarray(values, dtype=object))
    return (hashed & np.uint64(0x3FF)).astype(np.int64)


def write_partition_files(
    day: date,
    columns: dict[str, np.ndarray],
    planted_mask: np.ndarray,
    directory: Path,
    layout: str,
    max_files: int,
    row_group_rows: int,
    rng: np.random.Generator,
) -> list[Path]:
    columns, perm = order_columns(columns, layout, rng)
    planted_mask = planted_mask[perm]
    n = len(planted_mask)
    n_files = files_for(n, max_files)
    bounds = np.linspace(0, n, n_files + 1, dtype=np.int64)
    written = []
    for file_index in range(n_files):
        start, stop = int(bounds[file_index]), int(bounds[file_index + 1])
        if stop <= start:
            continue
        piece = {key: np.array(values[start:stop], copy=True) for key, values in columns.items()}
        # Sorted layouts keep one row group per file so the slice min/max stays narrow.
        group_rows = max(1, stop - start) if layout != "scattered" else row_group_rows
        if layout == "scattered" and day in QUERY_DAYS:
            inject_sentinels_masked(piece, planted_mask[start:stop], group_rows)
        path = directory / f"part-{day.isoformat()}-{file_index:03d}.parquet"
        write_table(path, piece, FACT_SCHEMA, group_rows)
        written.append(path)
    return written


def measure_bytes_per_row(
    pool: dict[str, np.ndarray],
    ndv: int,
    reserved: np.ndarray,
    stride: int,
    rng: np.random.Generator,
    scratch: Path,
) -> float:
    sample_n = 20_000
    columns = background_rows(
        sample_n, QUERY_DAYS[0], pool, ndv, reserved, stride, rng
    )
    scratch.mkdir(parents=True, exist_ok=True)
    path = scratch / "calibration.parquet"
    write_table(path, columns, FACT_SCHEMA, row_group_rows=sample_n)
    size = path.stat().st_size
    path.unlink()
    return size / sample_n


def check_row_group_span(paths: list[Path], request_imeis: set[str], layout: str) -> dict:
    spanning = 0
    checked = 0
    missing_stats = 0
    for path in paths:
        parquet = pq.ParquetFile(path)
        for group_index in range(parquet.num_row_groups):
            stats = parquet.metadata.row_group(group_index).column(2).statistics
            checked += 1
            if stats is None or not stats.has_min_max:
                missing_stats += 1
                continue
            low = stats.min.decode() if isinstance(stats.min, (bytes, bytearray)) else str(stats.min)
            high = stats.max.decode() if isinstance(stats.max, (bytes, bytearray)) else str(stats.max)
            if low <= min(request_imeis) and high >= max(request_imeis):
                spanning += 1
    result = {
        "query_day_files_checked": len(paths),
        "row_groups_checked": checked,
        "row_groups_spanning_all_request_imeis": spanning,
        "row_groups_missing_stats": missing_stats,
    }
    if layout == "scattered" and spanning != checked:
        raise RuntimeError(
            "Scattered layout did not force every checked row group to span the "
            f"request IMEIs ({spanning}/{checked}, missing stats {missing_stats})."
        )
    return result


def directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*.parquet"))


def save_model(synthesizer, metadata: Metadata, directory: Path, name: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    synthesizer.save(str(directory / f"{name}.pkl"))
    metadata.save_to_json(str(directory / f"{name}_metadata.json"))


def main() -> int:
    args = parse_args()
    if args.background_imei_ndv < 10_000:
        raise SystemExit("--background-imei-ndv must be at least 10000")
    if args.max_files_per_day < 1:
        raise SystemExit("--max-files-per-day must be at least 1")
    target_bytes = (
        int(args.target_mib * 1024 * 1024)
        if args.target_mib is not None
        else int(args.target_gib * 1024 * 1024 * 1024)
    )
    rng = np.random.default_rng(args.seed)
    output = args.output_dir
    if output.exists():
        raise SystemExit(f"Output directory already exists: {output}")
    fact_dir = output / "ipdr_db" / "ipdr_cisco_oct_2026"
    request_dir = output / "ipdr_rep" / "LGL_CGNAT_REQUEST_SPLIT_IMEI"
    fact_dir.mkdir(parents=True)
    request_dir.mkdir(parents=True)

    behavior_frame = behavior_seed(rng, args.seed_rows)
    behavior_model, behavior_meta, behavior_table = fit_gaussian(
        behavior_frame, args.seed, "ipdr behavior", "ipdr_behavior"
    )
    behavior_sample = behavior_model.sample(num_rows=args.pool_rows)[behavior_table]
    print_sdv_check(behavior_frame, behavior_sample)
    pool = clean_behavior(behavior_sample)

    context_frame = request_context_seed(rng, max(400, args.seed_rows // 10))
    context_model, context_meta, context_table = fit_gaussian(
        context_frame, args.seed + 1, "request context", "request_context"
    )
    context_sample = context_model.sample(num_rows=152)[context_table]
    save_model(behavior_model, behavior_meta, output / "models", "ipdr_behavior")
    save_model(context_model, context_meta, output / "models", "request_context")

    stride = max(1, IMEI_SPAN // (args.background_imei_ndv + 10_000))
    request_ids = np.linspace(0, args.background_imei_ndv - 1, 115, dtype=np.int64)
    if len(np.unique(request_ids)) != 115:
        raise SystemExit("Request catalog ids collided; increase --background-imei-ndv")
    requests, windows = build_requests(context_sample, request_ids, stride)
    planted = build_planted(windows, pool, stride, rng)
    join_summary = validate_join(planted, requests)
    print(
        "Join check passed: "
        f"{join_summary['output_rows']} output rows, "
        f"by date {join_summary['output_rows_by_date']}"
    )

    bytes_per_row = measure_bytes_per_row(
        pool,
        args.background_imei_ndv,
        request_ids,
        stride,
        rng,
        output / "_scratch",
    )
    shutil.rmtree(output / "_scratch")
    target_rows = max(1, int(round(target_bytes / bytes_per_row)))
    planted_per_day = {day: int(len(columns["imei_14"])) for day, columns in planted.items()}
    allocation = allocate_rows(target_rows, planted_per_day)
    row_group_rows = max(5_000, int((args.row_group_mib * 1024 * 1024) / bytes_per_row))
    print(
        f"Calibration {bytes_per_row:.1f} bytes/row. "
        f"Target {target_bytes / 1024**3:.2f} GiB -> {target_rows:,} rows, "
        f"row group {row_group_rows:,} rows."
    )

    written_query_files: list[Path] = []
    actual_rows = 0
    for day in PRODUCTION_PARTITION_ROWS:
        quota = allocation[day]
        planted_columns = planted.get(day)
        planted_n = 0 if planted_columns is None else len(planted_columns["imei_14"])
        background_n = quota - planted_n
        if background_n < 0:
            raise SystemExit(f"Partition {day} quota {quota} is below planted rows {planted_n}")
        parts = []
        masks = []
        if background_n:
            parts.append(
                background_rows(
                    background_n,
                    day,
                    pool,
                    args.background_imei_ndv,
                    request_ids,
                    stride,
                    rng,
                )
            )
            masks.append(np.zeros(background_n, dtype=bool))
        if planted_columns is not None:
            parts.append(planted_columns)
            masks.append(np.ones(planted_n, dtype=bool))
        columns = parts[0] if len(parts) == 1 else _concat_many(parts)
        planted_mask = np.concatenate(masks)
        paths = write_partition_files(
            day,
            columns,
            planted_mask,
            fact_dir,
            args.layout,
            args.max_files_per_day,
            row_group_rows,
            rng,
        )
        actual_rows += quota
        if day in QUERY_DAYS:
            written_query_files.extend(paths)
        print(f"Wrote {day.isoformat()} rows={quota:,} files={len(paths)}", flush=True)

    request_path = request_dir / "part-00000.parquet"
    write_requests(requests, request_path)
    imei_list = ", ".join(f"'{value}'" for value in sorted(set(requests["report_value"])))
    if len(imei_list) != 2068:
        raise SystemExit(f"Static IMEI list is {len(imei_list)} bytes, expected 2068")
    (output / "imei_in_list.txt").write_text(imei_list + "\n", encoding="utf-8")
    requests.to_csv(output / "requests.csv", index=False)

    span = check_row_group_span(
        written_query_files,
        set(requests["report_value"].astype(str)),
        args.layout,
    )
    fact_bytes = directory_size(fact_dir)
    request_bytes = request_path.stat().st_size
    manifest = {
        "databases": ["ipdr_db", "ipdr_rep"],
        "tables": {
            "ipdr_db.ipdr_cisco_oct_2026": {
                "rows": actual_rows,
                "parquet_bytes": fact_bytes,
                "layout": args.layout,
                "partition_rows": {day.isoformat(): allocation[day] for day in allocation},
            },
            "ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI": {
                "rows": 152,
                "parquet_bytes": request_bytes,
            },
        },
        "seed": args.seed,
        "sdv": {
            "synthesizer": "GaussianCopulaSynthesizer",
            "behavior_seed_rows": args.seed_rows,
            "behavior_pool_rows": args.pool_rows,
            "models": [
                "models/ipdr_behavior.pkl",
                "models/request_context.pkl",
            ],
        },
        "funnel": {
            "request_rows": 152,
            "distinct_imei": 115,
            "distinct_req_id": 152,
            "requests_overlapping_2026_10_11_to_17": 36,
            "seven_day_imei_matches": TARGET_IMEI_MATCHES,
            "seven_day_output_rows": join_summary["output_rows"],
            "output_rows_by_date": join_summary["output_rows_by_date"],
        },
        "row_group_stats": span,
        "identifiers": "synthetic; customer request IMEIs were not copied",
        "production_reference": {
            "fact_rows": 7491526854768,
            "fact_bytes": 389836735854797,
            "seven_day_rows": 1750220776124,
            "rows_leaving_scan": 66259,
            "output_rows": 8607,
            "note": (
                "The lab table keeps the production partition weights and the "
                "absolute 66259/8607 funnel. It does not reproduce 247,069 "
                "seven-day files or NameNode open latency."
            ),
        },
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(
        f"Fact parquet {fact_bytes / 1024**3:.3f} GiB, {actual_rows:,} rows. "
        f"Request parquet {request_bytes:,} bytes."
    )
    print(f"Manifest: {output / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BrokenPipeError:
        sys.exit(0)
