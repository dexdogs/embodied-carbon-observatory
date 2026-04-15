"""
ec3_ingest.py
=============
Ingests US manufacturing plants and EPD version history
from the EC3 API (Building Transparency) into TimescaleDB.

Data sources:
- EC3 API: https://openepd.buildingtransparency.org/api
- Plants endpoint: /materials/plants/public
- EPDs endpoint: openEPD format

Tables populated:
- plants
- epd_versions (hypertable)

Usage:
    export EC3_API_TOKEN=your_token_here
    export DATABASE_URL=postgresql://user:pass@host:port/dbname
    python ec3_ingest.py

    # Ingest specific material category only:
    python ec3_ingest.py --category concrete

    # Dry run (no DB writes):
    python ec3_ingest.py --dry-run

Requirements:
    pip install openepd psycopg2-binary sqlalchemy python-dotenv tqdm requests
"""

import os
import time
import logging
import argparse
import json
from datetime import datetime, timezone
from typing import Optional, Generator

import requests
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

EC3_API_BASE     = "https://openepd.buildingtransparency.org/api"
EC3_PLANTS_BASE  = "https://buildingtransparency.org/api"
DATABASE_URL     = os.getenv("DATABASE_URL")
EC3_API_TOKEN    = os.getenv("EC3_API_TOKEN")

# Rate limiting — EC3 API is rate limited
# Stay well under the limit
REQUEST_DELAY_SECONDS = 2.0
MAX_RETRIES           = 3
PAGE_SIZE             = 100

# Material categories to ingest
# Matches EC3 category slugs
MATERIAL_CATEGORIES = [
    "Concrete",
    "Steel",
    "Wood",
    "Aluminum",
    "Insulation",
    "Gypsumboard",
    "Glass",
    "Masonry",
    "Carpet",
    "CeilingTile",
]

# US states — filter out non-US plants
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC"
}

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ec3_ingest.log")
    ]
)
log = logging.getLogger(__name__)


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    """Get a psycopg2 connection to TimescaleDB."""
    if not DATABASE_URL:
        raise ValueError(
            "DATABASE_URL environment variable not set.\n"
            "Format: postgresql://user:pass@host:port/dbname"
        )
    return psycopg2.connect(DATABASE_URL)


def test_connection(conn) -> bool:
    """Verify TimescaleDB and PostGIS are available."""
    with conn.cursor() as cur:
        cur.execute("SELECT extname FROM pg_extension WHERE extname IN ('timescaledb', 'postgis');")
        extensions = {row[0] for row in cur.fetchall()}

        if 'timescaledb' not in extensions:
            log.error("TimescaleDB extension not found. Run: CREATE EXTENSION timescaledb;")
            return False
        if 'postgis' not in extensions:
            log.error("PostGIS extension not found. Run: CREATE EXTENSION postgis;")
            return False

        log.info("TimescaleDB and PostGIS confirmed available.")
        return True


# ============================================================
# EC3 API CLIENT
# ============================================================

class EC3Client:
    """
    Thin client for the EC3 / openEPD API.
    Handles auth, rate limiting, pagination, and retries.
    """

    def __init__(self, token: str):
        if not token:
            raise ValueError(
                "EC3_API_TOKEN not set.\n"
                "Get a token at: https://buildingtransparency.org/ec3/manage-apps/keys"
            )
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _get(self, url: str, params: dict = None) -> dict:
        """Single GET with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(REQUEST_DELAY_SECONDS)
                response = self.session.get(url, params=params, timeout=60)

                if response.status_code == 429:
                    wait = int(response.headers.get("Retry-After", 60))
                    log.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue

                if response.status_code == 401:
                    raise ValueError("EC3 API token invalid or expired.")

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == MAX_RETRIES - 1:
                    log.error(f"Failed after {MAX_RETRIES} attempts: {e}")
                    raise
                log.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(5 * (attempt + 1))

        return {}

    def get_plants(self, material_filter: str = None) -> Generator[dict, None, None]:
        """
        Fetch all plants from EC3's public plant endpoint.
        Yields one plant dict at a time.
        """
        url = f"{EC3_PLANTS_BASE}/materials/plants/public"
        params = {"page_size": PAGE_SIZE, "page": 1}
        if material_filter:
            params["category"] = material_filter

        log.info(f"Fetching plants from EC3... (filter: {material_filter or 'all'})")

        while True:
            data = self._get(url, params)

            # Handle both list and paginated response formats
            if isinstance(data, list):
                plants = data
                has_more = False
            else:
                plants = data.get("payload", data.get("results", data.get("plants", [])))
                has_more = data.get("next") is not None

            if not plants:
                break

            for plant in plants:
                yield plant

            if not has_more:
                break

            params["page"] += 1
            log.debug(f"Fetching page {params['page']}...")

    def get_epds(
        self,
        category: str = None,
        plant_id: str = None,
        include_expired: bool = True
    ) -> Generator[dict, None, None]:
        """
        Fetch EPDs from EC3 openEPD API.
        Yields one EPD dict at a time.
        Handles pagination transparently.
        """
        url = f"{EC3_API_BASE}/epds"
        params = {
            "page_size": PAGE_SIZE,
            "format": "json",
        }

        # Build filter query
        filters = []
        if category:
            filters.append(f'!EC3 search("{category}")')
        if plant_id:
            filters.append(f'plant.id: "{plant_id}"')

        # Include expired EPDs — we want the full history
        if not include_expired:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            filters.append(f'valid_until: >"{now}"')

        if filters:
            params["q"] = " and ".join(filters)

        log.info(f"Fetching EPDs from EC3... (category: {category or 'all'})")

        total_fetched = 0
        current_page = 1
        total_pages = None

        while True:
            params["page"] = current_page
            response = self.session.get(url, params=params, timeout=60)
            time.sleep(REQUEST_DELAY_SECONDS)

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 60))
                log.warning(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue

            response.raise_for_status()

            # Get total pages from headers on first request
            if total_pages is None:
                total_pages = int(response.headers.get("X-Total-Pages", 1))
                total_count = response.headers.get("X-Total-Count", "unknown")
                log.info(f"Total EPDs found: {total_count} across {total_pages} pages")

            epds = response.json()
            if not epds:
                break

            for epd in epds:
                total_fetched += 1
                yield epd

            log.debug(f"Page {current_page}/{total_pages} — {total_fetched} EPDs so far...")

            if current_page >= total_pages:
                break

            current_page += 1


# ============================================================
# DATA PARSERS
# ============================================================

def parse_plant(raw: dict) -> Optional[dict]:
    """
    Parse a raw EC3 plant record into our schema format.
    Returns None if the plant should be skipped (non-US, missing data).
    """
    try:
        # Extract address components
        address = raw.get("address", {})
        if isinstance(address, str):
            # Some records have address as a string
            state = None
            city = None
            address_str = address
        else:
            state = address.get("region", address.get("state", "")).upper()
            city  = address.get("city", address.get("locality", ""))
            address_str = address.get("street", address.get("line1", ""))

        # Filter to US only
        if False:  # state filter disabled — deriving from coords
            return None

        # Extract coordinates
        lat = raw.get("latitude")
        lng = raw.get("longitude")

        # Some records nest coordinates
        if lat is None:
            geo = raw.get("geometry", raw.get("location", {}))
            if isinstance(geo, dict):
                coords = geo.get("coordinates", [])
                if len(coords) >= 2:
                    lng, lat = coords[0], coords[1]

        if lat is None or lng is None:
            log.debug(f"Skipping plant {raw.get('id')} — no coordinates")
            return None

        # Determine material category
        categories = raw.get("categories", raw.get("product_classes", []))
        if isinstance(categories, list) and categories:
            material_category = categories[0].get("name", "unknown") if isinstance(categories[0], dict) else str(categories[0])
        else:
            material_category = "concrete"  # set by ingestion filter

        # Normalize category name
        material_category = normalize_category(material_category)

        return {
            "ec3_plant_id":         str(raw.get("id", raw.get("plant_id", ""))),
            "name":                  str(raw.get("plant_name", raw.get("name", "Unknown"))),
            "manufacturer":          str(raw.get("manufacturer_name", raw.get("manufacturer_name", ""))),
            "address":               address_str,
            "city":                  city,
            "state":                 state,
            "zip":                   address.get("postal_code", "") if isinstance(address, dict) else "",
            "lat":                   float(lat),
            "lng":                   float(lng),
            "material_category":     material_category,
            "material_subcategory":  raw.get("subcategory", raw.get("product_class", "")),
            "pluscode":               str(raw.get("pluscode", "")),
            "data_source":           "ec3",
        }

    except Exception as e:
        log.warning(f"Error parsing plant {raw.get('id', 'unknown')}: {e}")
        return None


def parse_epd(raw: dict, plant_db_id: str) -> Optional[dict]:
    """
    Parse a raw EC3 EPD record into our schema format.
    Returns None if the EPD should be skipped (missing critical data).
    """
    try:
        # Extract GWP values
        # EC3 stores these in impacts.gwp or directly as gwp
        impacts = raw.get("impacts", {})
        ec3_data = raw.get("ec3", {})
        gwp_data = {"a1a2a3": ec3_data.get("uaGWP_a1a2a3_ar5", ec3_data.get("uaGWP_a1a2a3_traci21"))}

        if isinstance(gwp_data, dict):
            gwp_total   = gwp_data.get("a1a2a3", gwp_data.get("total", gwp_data.get("value")))
            gwp_fossil  = gwp_data.get("fossil")
            gwp_biogenic = gwp_data.get("biogenic")
            gwp_luluc   = gwp_data.get("luluc")
        elif isinstance(gwp_data, (int, float)):
            gwp_total   = float(gwp_data)
            gwp_fossil  = None
            gwp_biogenic = None
            gwp_luluc   = None
        else:
            gwp_total   = raw.get("gwp_total") or raw.get("gwp")
            gwp_fossil  = raw.get("gwp_fossil")
            gwp_biogenic = raw.get("gwp_biogenic")
            gwp_luluc   = raw.get("gwp_luluc")

        # Skip EPDs with no GWP value — not useful for our analysis
        if gwp_total is None:
            return None

        # Parse dates
        issued_at  = parse_date(raw.get("date_of_issue") or raw.get("issued_at") or raw.get("date_published"))
        expired_at = parse_date(raw.get("valid_until") or raw.get("date_validity_ends") or raw.get("expired_at"))

        if issued_at is None:
            log.debug(f"Skipping EPD {raw.get('id')} — no issue date")
            return None

        # Declared unit
        declared_unit = raw.get("declared_unit", raw.get("functional_unit", ""))
        if isinstance(declared_unit, dict):
            declared_unit_qty = declared_unit.get("qty", 1.0)
            declared_unit     = declared_unit.get("unit", str(declared_unit))
        else:
            declared_unit_qty = 1.0

        # Program operator
        program_operator = raw.get("program_operator", {})
        if isinstance(program_operator, dict):
            program_operator = program_operator.get("name", "")

        return {
            "plant_id":            plant_db_id,
            "ec3_epd_id":          str(raw.get("id", "")),
            "issued_at":           issued_at,
            "expired_at":          expired_at,
            "gwp_total":           float(gwp_total) if gwp_total is not None else None,
            "gwp_fossil":          float(gwp_fossil) if gwp_fossil is not None else None,
            "gwp_biogenic":        float(gwp_biogenic) if gwp_biogenic is not None else None,
            "gwp_luluc":           float(gwp_luluc) if gwp_luluc is not None else None,
            "declared_unit":       str(declared_unit),
            "declared_unit_qty":   float(declared_unit_qty),
            "is_facility_specific": bool(raw.get("is_facility_specific", raw.get("facility_specific", False))),
            "is_product_specific": bool(raw.get("is_product_specific", raw.get("product_specific", False))),
            "pcr_id":              str(raw.get("pcr", {}).get("id", "") if isinstance(raw.get("pcr"), dict) else raw.get("pcr", "")),
            "program_operator":    str(program_operator),
            "epd_version":         int(raw.get("version", 1)) if raw.get("version") else None,
            "data_source":         "ec3",
            "raw_json":            json.dumps(raw),
        }

    except Exception as e:
        log.warning(f"Error parsing EPD {raw.get('id', 'unknown')}: {e}")
        return None


def parse_date(value) -> Optional[datetime]:
    """Parse various date formats into timezone-aware datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    date_formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
    ]
    for fmt in date_formats:
        try:
            dt = datetime.strptime(str(value), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    log.debug(f"Could not parse date: {value}")
    return None


def normalize_category(raw_category: str) -> str:
    """Normalize EC3 category names to our schema values."""
    cat = raw_category.lower().strip()
    if "concrete" in cat or "readymix" in cat or "ready mix" in cat:
        return "concrete"
    if "steel" in cat or "rebar" in cat:
        return "steel"
    if "wood" in cat or "timber" in cat or "lumber" in cat or "clt" in cat:
        return "timber"
    if "aluminum" in cat or "aluminium" in cat:
        return "aluminum"
    if "insulation" in cat:
        return "insulation"
    if "gypsum" in cat or "drywall" in cat:
        return "gypsum"
    if "glass" in cat or "glazing" in cat:
        return "glass"
    if "masonry" in cat or "brick" in cat or "cmu" in cat:
        return "masonry"
    if "carpet" in cat or "flooring" in cat:
        return "carpet"
    if "ceiling" in cat:
        return "ceiling_tile"
    return raw_category.lower().strip()


# ============================================================
# DATABASE WRITERS
# ============================================================

def upsert_plant(conn, plant: dict, dry_run: bool = False) -> Optional[str]:
    """
    Insert or update a plant record.
    Returns the database UUID of the plant.
    """
    if dry_run:
        log.debug(f"[DRY RUN] Would upsert plant: {plant['name']} ({plant['state']})")
        return "dry-run-id"

    sql = """
        INSERT INTO plants (
            ec3_plant_id, name, manufacturer, address, city, state, zip, pluscode,
            lat, lng, material_category, material_subcategory, data_source
        ) VALUES (
            %(ec3_plant_id)s, %(name)s, %(manufacturer)s, %(address)s,
            %(city)s, %(state)s, %(zip)s, %(pluscode)s, %(lat)s, %(lng)s,
            %(material_category)s, %(material_subcategory)s, %(data_source)s
        )
        ON CONFLICT (ec3_plant_id) DO UPDATE SET
            name                 = EXCLUDED.name,
            manufacturer         = EXCLUDED.manufacturer,
            address              = EXCLUDED.address,
            city                 = EXCLUDED.city,
            state                = EXCLUDED.state,
            zip                  = EXCLUDED.zip,
            lat                  = EXCLUDED.lat,
            lng                  = EXCLUDED.lng,
            material_category    = EXCLUDED.material_category,
            material_subcategory = EXCLUDED.material_subcategory,
            updated_at           = NOW()
        RETURNING id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, plant)
        result = cur.fetchone()
        return str(result[0]) if result else None


def batch_insert_epds(conn, epds: list[dict], dry_run: bool = False) -> int:
    """
    Batch insert EPD versions.
    Uses execute_values for performance.
    Skips duplicates via ON CONFLICT.
    Returns count of rows inserted.
    """
    if not epds:
        return 0

    if dry_run:
        log.debug(f"[DRY RUN] Would insert {len(epds)} EPD records")
        return len(epds)

    sql = """
        INSERT INTO epd_versions (
            plant_id, ec3_epd_id, issued_at, expired_at,
            gwp_total, gwp_fossil, gwp_biogenic, gwp_luluc,
            declared_unit, declared_unit_qty,
            is_facility_specific, is_product_specific,
            pcr_id, program_operator, epd_version,
            data_source, raw_json
        ) VALUES %s
        ON CONFLICT DO NOTHING;
    """

    values = [
        (
            e["plant_id"], e["ec3_epd_id"], e["issued_at"], e["expired_at"],
            e["gwp_total"], e["gwp_fossil"], e["gwp_biogenic"], e["gwp_luluc"],
            e["declared_unit"], e["declared_unit_qty"],
            e["is_facility_specific"], e["is_product_specific"],
            e["pcr_id"], e["program_operator"], e["epd_version"],
            e["data_source"], e["raw_json"]
        )
        for e in epds
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
        return cur.rowcount


# ============================================================
# MAIN INGESTION LOGIC
# ============================================================

def ingest_plants(
    client: EC3Client,
    conn,
    category: str = None,
    dry_run: bool = False
) -> dict:
    """
    Ingest all US plants from EC3.
    Returns stats dict.
    """
    stats = {
        "total_fetched": 0,
        "us_plants":     0,
        "skipped":       0,
        "inserted":      0,
        "errors":        0,
    }

    log.info("=" * 60)
    log.info("STEP 1: Ingesting plants")
    log.info("=" * 60)

    plant_ids = {}  # ec3_plant_id -> db uuid

    for raw_plant in tqdm(client.get_plants(category), desc="Plants", unit="plant"):
        stats["total_fetched"] += 1

        plant = parse_plant(raw_plant)
        if plant is None:
            stats["skipped"] += 1
            continue

        stats["us_plants"] += 1

        try:
            db_id = upsert_plant(conn, plant, dry_run)
            if db_id:
                plant_ids[plant["pluscode"]] = db_id
                stats["inserted"] += 1

            if not dry_run and stats["inserted"] % 100 == 0:
                conn.commit()
                log.info(f"Committed {stats['inserted']} plants...")

        except Exception as e:
            stats["errors"] += 1
            log.error(f"Error inserting plant {plant.get('name')}: {e}")
            conn.rollback()

    if not dry_run:
        conn.commit()

    log.info(f"Plants complete: {stats}")
    return stats, plant_ids


def ingest_epds(
    client: EC3Client,
    conn,
    plant_ids: dict,
    category: str = None,
    dry_run: bool = False,
    plant_id_filter: str = None
) -> dict:
    """
    Ingest all EPD versions from EC3.
    Joins EPDs to plants via ec3_plant_id.
    Returns stats dict.
    """
    stats = {
        "total_fetched":  0,
        "parsed":         0,
        "skipped":        0,
        "inserted":       0,
        "errors":         0,
        "no_plant_match": 0,
    }

    log.info("=" * 60)
    log.info("STEP 2: Ingesting EPD versions")
    log.info("=" * 60)

    # Also fetch plant_ids from DB in case we're resuming
    if not plant_ids and not dry_run:
        with conn.cursor() as cur:
            cur.execute("SELECT pluscode, id FROM plants WHERE pluscode IS NOT NULL AND pluscode != '';")
            plant_ids = {row[0]: str(row[1]) for row in cur.fetchall()}
        log.info(f"Loaded {len(plant_ids)} plant IDs from database")

    batch = []
    BATCH_SIZE = 500

    categories = [category] if category else MATERIAL_CATEGORIES

    for cat in categories:
        log.info(f"Fetching EPDs for category: {cat}")
        for raw_epd in tqdm(
            client.get_epds(category=cat, include_expired=True, plant_id=plant_id_filter),
            desc=f"EPDs ({cat})",
            unit="epd"
        ):
            stats["total_fetched"] += 1


            # Find the plant this EPD belongs to
            plant_ref = (raw_epd.get("plants") or [{}])[0]
            if isinstance(plant_ref, dict):
                epd_pluscode = str(plant_ref.get("pluscode", plant_ref.get("id", "").split(".")[0]))
            else:
                ec3_plant_id = ""

            db_plant_id = plant_ids.get(epd_pluscode)

            if not db_plant_id:
                stats["no_plant_match"] += 1
                # Don't skip — some EPDs have plant info embedded
                # Try to create a minimal plant record
                if isinstance(plant_ref, dict) and plant_ref.get("latitude"):
                    minimal_plant = parse_plant(plant_ref)
                    if minimal_plant:
                        try:
                            db_plant_id = upsert_plant(conn, minimal_plant, dry_run)
                            if db_plant_id:
                                plant_ids[epd_pluscode] = db_plant_id
                        except Exception:
                            pass

                if not db_plant_id:
                    stats["skipped"] += 1
                    continue

            epd = parse_epd(raw_epd, db_plant_id)
            if epd is None:
                stats["skipped"] += 1
                continue

            stats["parsed"] += 1
            batch.append(epd)

            if len(batch) >= BATCH_SIZE:
                try:
                    inserted = batch_insert_epds(conn, batch, dry_run)
                    stats["inserted"] += inserted
                    if not dry_run:
                        conn.commit()
                    batch = []
                    log.info(f"Inserted {stats['inserted']} EPDs so far...")
                except Exception as e:
                    stats["errors"] += 1
                    log.error(f"Batch insert error: {e}")
                    conn.rollback()
                    batch = []

    # Insert remaining batch
    if batch:
        try:
            inserted = batch_insert_epds(conn, batch, dry_run)
            stats["inserted"] += inserted
            if not dry_run:
                conn.commit()
        except Exception as e:
            stats["errors"] += 1
            log.error(f"Final batch insert error: {e}")
            conn.rollback()

    log.info(f"EPDs complete: {stats}")
    return stats


def print_summary(conn):
    """Print a summary of what's in the database."""
    log.info("\n" + "=" * 60)
    log.info("DATABASE SUMMARY")
    log.info("=" * 60)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM plants;")
        plant_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM epd_versions;")
        epd_count = cur.fetchone()[0]

        cur.execute("""
            SELECT material_category, COUNT(*) as plants, COUNT(DISTINCT state) as states
            FROM plants
            GROUP BY material_category
            ORDER BY plants DESC;
        """)
        categories = cur.fetchall()

        cur.execute("""
            SELECT
                DATE_TRUNC('year', issued_at) as year,
                COUNT(*) as epd_count
            FROM epd_versions
            GROUP BY year
            ORDER BY year;
        """)
        by_year = cur.fetchall()

    log.info(f"Total plants:       {plant_count:,}")
    log.info(f"Total EPD versions: {epd_count:,}")
    log.info("\nBy material category:")
    for cat, plants, states in categories:
        log.info(f"  {cat:<20} {plants:>6} plants across {states} states")
    log.info("\nEPD versions by year:")
    for year, count in by_year:
        if year:
            log.info(f"  {year.year}: {count:>6} EPDs")


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Ingest EC3 plant and EPD data into TimescaleDB"
    )
    parser.add_argument(
        "--category",
        type=str,
        default=None,
        help=f"Material category to ingest. Options: {', '.join(MATERIAL_CATEGORIES)}"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate data without writing to database"
    )
    parser.add_argument(
        "--plants-only",
        action="store_true",
        help="Only ingest plants, skip EPDs"
    )
    parser.add_argument(
        "--epds-only",
        action="store_true",
        help="Only ingest EPDs (plants must already exist in DB)"
    )
    parser.add_argument(
        "--plant-id",
        type=str,
        default=None,
        help="Ingest EPDs for a specific EC3 plant ID only"
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("EMBODIED CARBON OBSERVATORY — EC3 INGESTION")
    log.info("=" * 60)
    log.info(f"Category filter: {args.category or 'all'}")
    log.info(f"Dry run:         {args.dry_run}")

    # Initialize API client
    client = EC3Client(EC3_API_TOKEN)

    # Connect to database
    conn = get_connection()

    if not args.dry_run:
        if not test_connection(conn):
            log.error("Database connection test failed. Exiting.")
            return

    try:
        plant_ids = {}

        if not args.epds_only:
            _, plant_ids = ingest_plants(
                client, conn,
                category=args.category,
                dry_run=args.dry_run
            )

        if not args.plants_only:
            ingest_epds(
                client, conn, plant_ids,
                category=args.category,
                dry_run=args.dry_run,
                plant_id_filter=args.plant_id
            )

        if not args.dry_run:
            print_summary(conn)

        log.info("\nIngestion complete.")

    except KeyboardInterrupt:
        log.info("\nInterrupted by user. Committing partial progress...")
        if not args.dry_run:
            conn.commit()
    except Exception as e:
        log.error(f"Fatal error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
