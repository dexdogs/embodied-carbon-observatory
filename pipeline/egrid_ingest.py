"""
egrid_ingest.py
===============
Downloads and ingests EPA eGRID annual grid carbon intensity data
into TimescaleDB grid_carbon hypertable.

Covers all available eGRID years: 1996-2023
Source: https://www.epa.gov/egrid

Tables populated:
- grid_carbon (hypertable)

Usage:
    python egrid_ingest.py
    python egrid_ingest.py --year 2023
    python egrid_ingest.py --dry-run

Requirements:
    pip install psycopg2-binary python-dotenv tqdm requests openpyxl pandas
"""

import os
import io
import logging
import argparse
import time
from datetime import datetime, timezone
from typing import Optional
from pathlib import Path

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("egrid_ingest.log")
    ]
)
log = logging.getLogger(__name__)

# ============================================================
# eGRID FILE REGISTRY
# All available eGRID Excel files from EPA
# Direct download URLs — confirmed public access
# ============================================================

EGRID_FILES = {
    2023: "https://www.epa.gov/system/files/documents/2025-06/egrid2023_data_rev2.xlsx",
    2022: "https://www.epa.gov/system/files/documents/2024-01/egrid2022_data.xlsx",
    2021: "https://www.epa.gov/system/files/documents/2023-01/eGRID2021_data.xlsx",
    2020: "https://www.epa.gov/system/files/documents/2022-09/eGRID2020_Data_v2.xlsx",
    2019: "https://www.epa.gov/sites/default/files/2021-02/egrid2019_data.xlsx",
    2018: "https://www.epa.gov/sites/default/files/2020-03/egrid2018_data_v2.xlsx",
    2016: "https://www.epa.gov/system/files/documents/2020-01/egrid2016_data.xlsx",
    2014: "https://www.epa.gov/system/files/documents/2020-01/egrid2014_data.xlsx",
    2012: "https://www.epa.gov/system/files/documents/2020-01/egrid2012_data.xlsx",
    2010: "https://www.epa.gov/system/files/documents/2020-01/egrid2010_data.xlsx",
    2009: "https://www.epa.gov/system/files/documents/2020-01/egrid2009_data.xlsx",
    2007: "https://www.epa.gov/system/files/documents/2020-01/egrid2007_data.xlsx",
    2005: "https://www.epa.gov/system/files/documents/2020-01/egrid2005_data.xlsx",
}

# eGRID subregion acronyms to full names
# Used for documentation and display
SUBREGION_NAMES = {
    "AKGD": "ASCC Alaska Grid",
    "AKMS": "ASCC Miscellaneous",
    "AZNM": "WECC Southwest",
    "CAMX": "WECC California",
    "ERCT": "ERCOT Texas",
    "FRCC": "FRCC Florida",
    "HIMS": "HICC Miscellaneous",
    "HIOA": "HICC Oahu",
    "MROE": "MRO East",
    "MROW": "MRO West",
    "NEWE": "NPCC New England",
    "NWPP": "WECC Northwest",
    "NYCW": "NPCC NYC/Westchester",
    "NYLI": "NPCC Long Island",
    "NYUP": "NPCC Upstate NY",
    "PRMS": "Puerto Rico Misc",
    "RFCE": "RFC East",
    "RFCM": "RFC Michigan",
    "RFCW": "RFC West",
    "RMPA": "WECC Rockies",
    "SPNO": "SPP North",
    "SPSO": "SPP South",
    "SRMV": "SERC Mississippi Valley",
    "SRMW": "SERC Midwest",
    "SRSO": "SERC South",
    "SRTV": "SERC Tennessee Valley",
    "SRVC": "SERC Virginia/Carolina",
}

# State to primary eGRID subregion mapping
# Used when we need to assign a subregion from state only
STATE_TO_SUBREGION = {
    "ME": "NEWE", "NH": "NEWE", "VT": "NEWE",
    "MA": "NEWE", "RI": "NEWE", "CT": "NEWE",
    "NY": "NYCW",
    "PA": "RFCE", "NJ": "RFCE", "MD": "RFCE",
    "DE": "RFCE", "DC": "RFCE",
    "VA": "SRVC", "NC": "SRVC",
    "WV": "RFCW", "OH": "RFCW",
    "MI": "RFCM",
    "IN": "RFCW", "IL": "RFCW",
    "WI": "MROE",
    "KY": "SRTV", "TN": "SRTV",
    "SC": "SRVC",
    "GA": "SRSO", "FL": "FRCC",
    "AL": "SRSO", "MS": "SRMV",
    "AR": "SRMV", "LA": "SRMV",
    "TX": "ERCT",
    "MN": "MROW", "IA": "MROW",
    "MO": "SRMW", "ND": "MROW",
    "SD": "MROW", "NE": "MROW",
    "KS": "SPNO", "OK": "SPSO",
    "MT": "NWPP", "WY": "RMPA",
    "CO": "RMPA", "UT": "NWPP",
    "NV": "NWPP", "AZ": "AZNM",
    "NM": "AZNM",
    "WA": "NWPP", "OR": "NWPP",
    "ID": "NWPP",
    "CA": "CAMX",
    "AK": "AKGD",
    "HI": "HIOA",
}

# Column name mappings across eGRID versions
# EPA changed column names between releases — this normalizes them
COLUMN_MAPPINGS = {
    # Subregion acronym
    "subregion": [
        "SUBRGN", "SRC2ERTA", "subregion_acronym",
        "eGRID Subregion Acronym"
    ],
    # CO2 rate (lb/MWh)
    "co2_rate": [
        "SRCO2RTA", "SRC2ERTA", "co2_rate_lb_per_mwh",
        "Subregion Annual CO2 Total Output Emission Rate (lb/MWh)"
    ],
    # CH4 rate (lb/MWh)
    "ch4_rate": [
        "SRCH4RTA", "ch4_rate_lb_per_mwh",
        "Subregion Annual CH4 Total Output Emission Rate (lb/MWh)"
    ],
    # N2O rate (lb/MWh)
    "n2o_rate": [
        "SRN2ORTA", "n2o_rate_lb_per_mwh",
        "Subregion Annual N2O Total Output Emission Rate (lb/MWh)"
    ],
    # CO2e rate (lb/MWh) — combined
    "co2e_rate": [
        "SRCO2RTA",    # annual output emission rate lb/MWh (confirmed 2022)
        "SRCO2ERTA",   # alternate name some vintages
        "SRC2ERTA",    # older vintages
        "co2e_rate_lb_per_mwh",
        "Subregion Annual CO2 Equivalent Total Output Emission Rate (lb/MWh)"
    ],
    # Resource mix percentages
    "pct_coal":    ["SRCLPR",  "coal_pct",    "Subregion Annual Coal Percent (resource mix)"],
    "pct_gas":     ["SRGAPR",  "gas_pct",     "Subregion Annual Gas Percent (resource mix)"],
    "pct_nuclear": ["SRNUCPR", "nuclear_pct", "Subregion Annual Nuclear Percent (resource mix)"],
    "pct_hydro":   ["SRHYDPR", "hydro_pct",   "Subregion Annual Hydro Percent (resource mix)"],
    "pct_wind":    ["SRWINPR", "wind_pct",    "Subregion Annual Wind Percent (resource mix)"],
    "pct_solar":   ["SRSOLPR", "solar_pct",   "Subregion Annual Solar Percent (resource mix)"],
    "pct_other":   ["SROTHRPR","other_pct",   "Subregion Annual Other Percent (resource mix)"],
}


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set.")
    return psycopg2.connect(DATABASE_URL)


# ============================================================
# DOWNLOADER
# ============================================================

def download_egrid_file(year: int, cache_dir: str = "egrid_cache") -> Optional[bytes]:
    """
    Download an eGRID Excel file for a given year.
    Caches to disk so re-runs don't re-download.
    Returns file bytes or None if download fails.
    """
    Path(cache_dir).mkdir(exist_ok=True)
    cache_path = Path(cache_dir) / f"egrid{year}_data.xlsx"

    # Return cached version if available
    if cache_path.exists():
        log.info(f"Using cached eGRID {year} file: {cache_path}")
        return cache_path.read_bytes()

    url = EGRID_FILES.get(year)
    if not url:
        log.warning(f"No eGRID URL registered for year {year}")
        return None

    log.info(f"Downloading eGRID {year} from EPA...")

    try:
        response = requests.get(url, timeout=120, stream=True)
        response.raise_for_status()

        content = response.content
        cache_path.write_bytes(content)
        log.info(f"Downloaded and cached: {cache_path} ({len(content):,} bytes)")
        return content

    except requests.exceptions.RequestException as e:
        log.error(f"Failed to download eGRID {year}: {e}")
        return None


# ============================================================
# PARSER
# ============================================================

def find_column(df: pd.DataFrame, field: str) -> Optional[str]:
    """
    Find the actual column name in a DataFrame for a logical field.
    Tries all known aliases for that field.
    Returns the matched column name or None.
    """
    candidates = COLUMN_MAPPINGS.get(field, [])
    df_cols_upper = {col.upper(): col for col in df.columns}

    for candidate in candidates:
        if candidate in df.columns:
            return candidate
        if candidate.upper() in df_cols_upper:
            return df_cols_upper[candidate.upper()]

    # Fuzzy fallback — check if any column contains key words
    field_keywords = {
        "subregion":  ["SUBRGN", "SUBREGION"],
        "co2e_rate":  ["CO2ERTA", "CO2ERTE"],  # must end in RTA/RTE = rate, not totals
        "co2_rate":   ["CO2R"],
        "ch4_rate":   ["CH4R"],
        "n2o_rate":   ["N2OR"],
        "pct_coal":   ["COAL"],
        "pct_gas":    ["GAS", "NGAS"],
        "pct_nuclear":["NUC"],
        "pct_hydro":  ["HYD"],
        "pct_wind":   ["WIN"],
        "pct_solar":  ["SOL"],
        "pct_other":  ["OTH"],
    }

    keywords = field_keywords.get(field, [])
    for col in df.columns:
        for kw in keywords:
            if kw in col.upper():
                return col

    return None


def find_subregion_sheet(xlsx_bytes: bytes, year: int) -> Optional[pd.DataFrame]:
    """
    Find and load the subregion-level sheet from an eGRID Excel file.
    EPA uses different sheet names across years.
    """
    try:
        xl = pd.ExcelFile(io.BytesIO(xlsx_bytes))
        sheet_names = xl.sheet_names

        log.debug(f"eGRID {year} sheets: {sheet_names}")

        # Try known sheet name patterns
        candidates = [
            "SRL",           # Most common — Subregion Level
            "SUBREGION",
            "Subregion",
            "SR",
            f"SRL{year}",
            f"eGRID{year} SUBRGN",
        ]

        for candidate in candidates:
            if candidate in sheet_names:
                log.info(f"Using sheet '{candidate}' for eGRID {year}")
                df = xl.parse(candidate, header=1)  # Row 1 is usually the header
                return df

        # If none matched, try to find any sheet with "SRL" or "SUBR"
        for sheet in sheet_names:
            if "SRL" in sheet.upper() or "SUBR" in sheet.upper():
                log.info(f"Using sheet '{sheet}' for eGRID {year}")
                df = xl.parse(sheet, header=1)
                return df

        log.warning(f"Could not find subregion sheet in eGRID {year}. Sheets: {sheet_names}")
        return None

    except Exception as e:
        log.error(f"Error parsing eGRID {year} Excel file: {e}")
        return None


def parse_egrid_year(xlsx_bytes: bytes, year: int) -> list[dict]:
    """
    Parse a single year's eGRID Excel file into a list of records
    ready for insertion into grid_carbon.
    """
    df = find_subregion_sheet(xlsx_bytes, year)
    if df is None:
        return []

    # Drop rows where subregion is null
    subregion_col = find_column(df, "subregion")
    if subregion_col is None:
        log.error(f"Cannot find subregion column in eGRID {year}")
        log.debug(f"Available columns: {list(df.columns)[:20]}")
        return []

    df = df.dropna(subset=[subregion_col])
    df = df[df[subregion_col].astype(str).str.strip() != ""]

    records = []
    year_ts = datetime(year, 1, 1, tzinfo=timezone.utc)

    for _, row in df.iterrows():
        subregion = str(row[subregion_col]).strip().upper()

        # Skip non-subregion rows (totals, headers, etc.)
        if len(subregion) < 3 or subregion in ("NAN", "SUBRGN", "SUBREGION"):
            continue

        def get_val(field: str) -> Optional[float]:
            col = find_column(df, field)
            if col is None:
                return None
            val = row.get(col)
            if pd.isna(val):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # co2e_rate is required — skip if missing
        co2e_rate = get_val("co2e_rate")
        if co2e_rate is None:
            # Fall back to computing from CO2 rate if available
            co2_rate = get_val("co2_rate")
            if co2_rate is not None:
                co2e_rate = co2_rate  # approximate
            else:
                log.debug(f"Skipping {subregion} {year} — no CO2e rate")
                continue

        records.append({
            "egrid_subregion":        subregion,
            "state":                  None,  # subregion level, not state level
            "year":                   year_ts,
            "co2_rate_lb_per_mwh":    get_val("co2_rate"),
            "ch4_rate_lb_per_mwh":    get_val("ch4_rate"),
            "n2o_rate_lb_per_mwh":    get_val("n2o_rate"),
            "co2e_rate_lb_per_mwh":   co2e_rate,
            "resource_mix_pct_coal":  get_val("pct_coal"),
            "resource_mix_pct_gas":   get_val("pct_gas"),
            "resource_mix_pct_nuclear": get_val("pct_nuclear"),
            "resource_mix_pct_hydro": get_val("pct_hydro"),
            "resource_mix_pct_wind":  get_val("pct_wind"),
            "resource_mix_pct_solar": get_val("pct_solar"),
            "resource_mix_pct_other": get_val("pct_other"),
            "egrid_version":          f"eGRID{year}",
            "data_source":            "epa_egrid",
        })

    log.info(f"Parsed {len(records)} subregion records for eGRID {year}")
    return records


# ============================================================
# DATABASE WRITER
# ============================================================

def insert_grid_carbon(conn, records: list[dict], dry_run: bool = False) -> int:
    """
    Batch insert grid_carbon records.
    Upserts on (egrid_subregion, year) — safe to re-run.
    Returns count inserted/updated.
    """
    if not records:
        return 0

    if dry_run:
        log.info(f"[DRY RUN] Would insert {len(records)} grid_carbon records")
        for r in records[:3]:
            log.info(f"  Sample: {r['egrid_subregion']} {r['year'].year} "
                     f"co2e={r['co2e_rate_lb_per_mwh']:.1f} lb/MWh")
        return len(records)

    sql = """
        INSERT INTO grid_carbon (
            egrid_subregion, state, year,
            co2_rate_lb_per_mwh, ch4_rate_lb_per_mwh, n2o_rate_lb_per_mwh,
            co2e_rate_lb_per_mwh,
            resource_mix_pct_coal, resource_mix_pct_gas,
            resource_mix_pct_nuclear, resource_mix_pct_hydro,
            resource_mix_pct_wind, resource_mix_pct_solar,
            resource_mix_pct_other,
            egrid_version, data_source
        ) VALUES %s
        ON CONFLICT (egrid_subregion, year)
        DO UPDATE SET
            co2_rate_lb_per_mwh    = EXCLUDED.co2_rate_lb_per_mwh,
            ch4_rate_lb_per_mwh    = EXCLUDED.ch4_rate_lb_per_mwh,
            n2o_rate_lb_per_mwh    = EXCLUDED.n2o_rate_lb_per_mwh,
            co2e_rate_lb_per_mwh   = EXCLUDED.co2e_rate_lb_per_mwh,
            resource_mix_pct_coal  = EXCLUDED.resource_mix_pct_coal,
            resource_mix_pct_gas   = EXCLUDED.resource_mix_pct_gas,
            resource_mix_pct_nuclear = EXCLUDED.resource_mix_pct_nuclear,
            resource_mix_pct_hydro = EXCLUDED.resource_mix_pct_hydro,
            resource_mix_pct_wind  = EXCLUDED.resource_mix_pct_wind,
            resource_mix_pct_solar = EXCLUDED.resource_mix_pct_solar,
            resource_mix_pct_other = EXCLUDED.resource_mix_pct_other,
            egrid_version          = EXCLUDED.egrid_version;
    """

    values = [
        (
            r["egrid_subregion"], r["state"], r["year"],
            r["co2_rate_lb_per_mwh"], r["ch4_rate_lb_per_mwh"],
            r["n2o_rate_lb_per_mwh"], r["co2e_rate_lb_per_mwh"],
            r["resource_mix_pct_coal"], r["resource_mix_pct_gas"],
            r["resource_mix_pct_nuclear"], r["resource_mix_pct_hydro"],
            r["resource_mix_pct_wind"], r["resource_mix_pct_solar"],
            r["resource_mix_pct_other"],
            r["egrid_version"], r["data_source"]
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
        count = cur.rowcount

    conn.commit()
    return count


def add_unique_constraint_if_missing(conn):
    """
    Add unique constraint on (egrid_subregion, year) if not exists.
    Required for the ON CONFLICT upsert to work.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM pg_constraint
            WHERE conname = 'grid_carbon_subregion_year_unique';
        """)
        if cur.fetchone()[0] == 0:
            cur.execute("""
                ALTER TABLE grid_carbon
                ADD CONSTRAINT grid_carbon_subregion_year_unique
                UNIQUE (egrid_subregion, year);
            """)
            conn.commit()
            log.info("Added unique constraint on grid_carbon(egrid_subregion, year)")


def print_summary(conn):
    """Print summary of grid_carbon table."""
    log.info("\n" + "=" * 60)
    log.info("GRID CARBON SUMMARY")
    log.info("=" * 60)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM grid_carbon;")
        total = cur.fetchone()[0]

        cur.execute("""
            SELECT
                EXTRACT(year FROM year)::int AS yr,
                COUNT(*) AS subregions,
                ROUND(AVG(co2e_rate_lb_per_mwh)::numeric, 1) AS avg_co2e,
                ROUND(MIN(co2e_rate_lb_per_mwh)::numeric, 1) AS min_co2e,
                ROUND(MAX(co2e_rate_lb_per_mwh)::numeric, 1) AS max_co2e
            FROM grid_carbon
            GROUP BY yr
            ORDER BY yr;
        """)
        rows = cur.fetchall()

    log.info(f"Total records: {total}")
    log.info(f"\n{'Year':<6} {'Subregions':<12} {'Avg CO2e':<12} {'Min':<8} {'Max':<8}")
    log.info("-" * 50)
    for yr, subregions, avg, mn, mx in rows:
        log.info(f"{yr:<6} {subregions:<12} {avg:<12} {mn:<8} {mx:<8}")

    # Show the decarbonization signal — this is the story
    log.info("\nNew England (NEWE) grid decarbonization over time:")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                EXTRACT(year FROM year)::int AS yr,
                ROUND(co2e_rate_lb_per_mwh::numeric, 1) AS co2e_rate,
                ROUND((COALESCE(resource_mix_pct_wind,0) + COALESCE(resource_mix_pct_solar,0) + COALESCE(resource_mix_pct_hydro,0))::numeric, 1) AS pct_renewable
            FROM grid_carbon
            WHERE egrid_subregion = 'NEWE'
            ORDER BY year;
        """)
        newe = cur.fetchall()

    if newe:
        for yr, co2e, pct_ren in newe:
            bar = "█" * int((co2e or 0) / 30)
            log.info(f"  {yr}: {co2e:>6} lb/MWh {bar}")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Ingest EPA eGRID data into TimescaleDB grid_carbon table"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Ingest a specific year only (default: all available years)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate without writing to database"
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default="egrid_cache",
        help="Directory to cache downloaded Excel files"
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("EMBODIED CARBON OBSERVATORY — eGRID INGESTION")
    log.info("=" * 60)

    years_to_process = [args.year] if args.year else sorted(EGRID_FILES.keys())
    log.info(f"Years to process: {years_to_process}")

    conn = get_connection() if not args.dry_run else None

    if conn:
        add_unique_constraint_if_missing(conn)

    total_inserted = 0

    for year in tqdm(years_to_process, desc="eGRID years", unit="year"):
        log.info(f"\nProcessing eGRID {year}...")

        xlsx_bytes = download_egrid_file(year, args.cache_dir)
        if xlsx_bytes is None:
            log.warning(f"Skipping eGRID {year} — download failed")
            continue

        records = parse_egrid_year(xlsx_bytes, year)
        if not records:
            log.warning(f"No records parsed for eGRID {year}")
            continue

        if conn:
            inserted = insert_grid_carbon(conn, records, args.dry_run)
            total_inserted += inserted
            log.info(f"eGRID {year}: {inserted} records inserted/updated")
        else:
            # Dry run
            insert_grid_carbon(None, records, dry_run=True)

        # Be polite to EPA servers
        time.sleep(1)

    if conn:
        print_summary(conn)
        conn.close()

    log.info(f"\nTotal records inserted: {total_inserted}")
    log.info("eGRID ingestion complete.")


if __name__ == "__main__":
    main()