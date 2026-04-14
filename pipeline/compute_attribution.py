"""
compute_attribution.py
======================
The core analytical engine of the Embodied Carbon Observatory.

For every plant with multiple EPD versions, computes:
- Total GWP delta between versions
- How much of that delta came from grid decarbonization
- How much came from actual manufacturing process improvement

This is the question nobody has answered at scale:
"Is this plant actually getting cleaner, or is the grid just getting cleaner?"

Tables read:
- plants
- epd_versions (hypertable)
- grid_carbon (hypertable)

Tables written:
- gwp_attribution

Usage:
    python compute_attribution.py
    python compute_attribution.py --category concrete
    python compute_attribution.py --dry-run

Requirements:
    pip install psycopg2-binary python-dotenv tqdm
"""

import os
import logging
import argparse
from datetime import timezone
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
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
        logging.FileHandler("compute_attribution.log")
    ]
)
log = logging.getLogger(__name__)


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set.")
    return psycopg2.connect(DATABASE_URL)


# ============================================================
# ATTRIBUTION LOGIC
# ============================================================

def compute_grid_contribution(
    gwp_start: float,
    gwp_end: float,
    grid_start: float,
    grid_end: float,
) -> dict:
    """
    Decompose GWP change into grid-driven vs process-driven components.

    Method:
    The A3 manufacturing stage GWP is directly proportional to electricity
    consumed × grid carbon intensity. When the grid gets cleaner, A3 drops
    even if the plant does nothing.

    We estimate the grid contribution as:
        grid_contribution = gwp_start × (grid_delta / grid_start)

    Where grid_delta = grid_end - grid_start

    The process contribution is the remainder:
        process_contribution = total_delta - grid_contribution

    This is an approximation — it assumes electricity is a fixed
    fraction of total GWP, which is roughly true for most material
    categories. Confidence is lower for materials where electricity
    is a small fraction (e.g. concrete, where cement clinker dominates).

    Returns dict with all attribution fields.
    """
    gwp_delta_total = gwp_end - gwp_start
    grid_delta      = grid_end - grid_start

    # Estimate grid contribution
    if grid_start and grid_start > 0:
        grid_fraction      = grid_delta / grid_start
        gwp_delta_grid     = gwp_start * grid_fraction
    else:
        gwp_delta_grid     = 0.0

    gwp_delta_process = gwp_delta_total - gwp_delta_grid

    # Percentage calculations
    pct_change_total = (gwp_delta_total / gwp_start * 100) if gwp_start else None

    if gwp_delta_total != 0:
        pct_from_grid    = (gwp_delta_grid / gwp_delta_total * 100)
        pct_from_process = (gwp_delta_process / gwp_delta_total * 100)
    else:
        pct_from_grid    = 0.0
        pct_from_process = 0.0

    # Confidence assessment
    # High: facility-specific EPD, large delta, grid data available
    # Medium: product-specific EPD, moderate delta
    # Low: industry average EPD, small delta or missing grid data
    if grid_start and grid_end:
        confidence = "high" if abs(pct_change_total or 0) > 5 else "medium"
    else:
        confidence = "low"

    return {
        "gwp_delta_total":       gwp_delta_total,
        "grid_co2e_start":       grid_start,
        "grid_co2e_end":         grid_end,
        "grid_co2e_delta":       grid_delta,
        "gwp_delta_grid":        gwp_delta_grid,
        "gwp_delta_process":     gwp_delta_process,
        "pct_change_total":      pct_change_total,
        "pct_from_grid":         pct_from_grid,
        "pct_from_process":      pct_from_process,
        "attribution_confidence": confidence,
    }


# ============================================================
# MAIN COMPUTATION
# ============================================================

def get_plants_with_multiple_epds(conn, category: str = None) -> list[dict]:
    """
    Get all plants that have at least 2 EPD versions.
    These are the plants where we can compute attribution.
    """
    sql = """
        SELECT
            p.id           AS plant_id,
            p.name         AS plant_name,
            p.state,
            p.egrid_subregion,
            p.material_category,
            COUNT(e.id)    AS epd_count,
            MIN(e.issued_at) AS first_epd,
            MAX(e.issued_at) AS last_epd
        FROM plants p
        JOIN epd_versions e ON e.plant_id = p.id
        WHERE e.gwp_total IS NOT NULL
        {category_filter}
        GROUP BY p.id, p.name, p.state, p.egrid_subregion, p.material_category
        HAVING COUNT(e.id) >= 2
        ORDER BY COUNT(e.id) DESC;
    """

    category_filter = f"AND p.material_category = '{category}'" if category else ""
    sql = sql.format(category_filter=category_filter)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


def get_epd_versions_for_plant(conn, plant_id: str) -> list[dict]:
    """
    Get one canonical EPD per (year, declared_unit) for a plant.
    Uses median GWP across all EPDs of that unit in that year.
    Only returns unit/years with data in 2+ distinct years (needed for attribution).
    """
    sql = """
        WITH yearly AS (
            SELECT
                plant_id,
                declared_unit,
                EXTRACT(year FROM issued_at)::int   AS epd_year,
                MIN(issued_at)                       AS issued_at,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gwp_total) AS gwp_total,
                COUNT(*)                             AS epd_count
            FROM epd_versions
            WHERE plant_id = %s
              AND gwp_total IS NOT NULL
              AND declared_unit IS NOT NULL
            GROUP BY plant_id, declared_unit, EXTRACT(year FROM issued_at)::int
        ),
        units_ranked AS (
            SELECT declared_unit,
                   COUNT(DISTINCT epd_year) AS year_count,
                   SUM(epd_count)           AS total_epds
            FROM yearly
            GROUP BY declared_unit
            HAVING COUNT(DISTINCT epd_year) >= 2
            ORDER BY COUNT(DISTINCT epd_year) DESC, SUM(epd_count) DESC
            LIMIT 1
        )
        SELECT
            y.plant_id,
            y.declared_unit,
            y.epd_year,
            y.issued_at,
            y.gwp_total,
            y.epd_count,
            NULL::text AS ec3_epd_id,
            NULL::boolean AS is_facility_specific,
            NULL::boolean AS is_product_specific
        FROM yearly y
        JOIN units_ranked u USING (declared_unit)
        ORDER BY y.epd_year ASC;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (plant_id,))
        return cur.fetchall()


def get_grid_carbon_for_subregion_year(
    conn,
    subregion: str,
    year: int
) -> Optional[float]:
    """
    Get the grid CO2e rate for a subregion and year.
    If exact year not available, use nearest available year.
    """
    if not subregion:
        return None

    sql = """
        SELECT co2e_rate_lb_per_mwh
        FROM grid_carbon
        WHERE egrid_subregion = %s
          AND EXTRACT(year FROM year) = %s
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (subregion, year))
        result = cur.fetchone()
        if result:
            return result[0]

        # Nearest year fallback
        cur.execute("""
            SELECT co2e_rate_lb_per_mwh
            FROM grid_carbon
            WHERE egrid_subregion = %s
            ORDER BY ABS(EXTRACT(year FROM year) - %s)
            LIMIT 1;
        """, (subregion, year))
        result = cur.fetchone()
        return result[0] if result else None


def attribution_already_computed(conn, plant_id: str, period_start, period_end) -> bool:
    """Check if attribution for this plant/period pair already exists."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM gwp_attribution
            WHERE plant_id = %s
              AND period_start = %s
              AND period_end = %s;
        """, (plant_id, period_start, period_end))
        return cur.fetchone()[0] > 0


def insert_attribution_records(
    conn,
    records: list[dict],
    dry_run: bool = False
) -> int:
    """Batch insert attribution records."""
    if not records:
        return 0

    if dry_run:
        log.info(f"[DRY RUN] Would insert {len(records)} attribution records")
        for r in records[:2]:
            log.info(
                f"  Plant {r['plant_id'][:8]}... "
                f"GWP {r['gwp_start']:.1f} → {r['gwp_end']:.1f} "
                f"({r['pct_change_total']:+.1f}%) "
                f"Grid: {r['pct_from_grid']:.0f}% "
                f"Process: {r['pct_from_process']:.0f}%"
            )
        return len(records)

    sql = """
        INSERT INTO gwp_attribution (
            plant_id, period_start, period_end,
            gwp_start, gwp_end, gwp_delta_total,
            grid_co2e_start, grid_co2e_end, grid_co2e_delta,
            gwp_delta_grid, gwp_delta_process,
            pct_change_total, pct_from_grid, pct_from_process,
            attribution_confidence, declared_unit
        ) VALUES %s
        ON CONFLICT DO NOTHING;
    """

    values = [
        (
            r["plant_id"],
            r["period_start"], r["period_end"],
            r["gwp_start"], r["gwp_end"], r["gwp_delta_total"],
            r["grid_co2e_start"], r["grid_co2e_end"], r["grid_co2e_delta"],
            r["gwp_delta_grid"], r["gwp_delta_process"],
            r["pct_change_total"], r["pct_from_grid"], r["pct_from_process"],
            r["attribution_confidence"], r.get("declared_unit")
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
        count = cur.rowcount

    conn.commit()
    return count


def compute_all_attributions(
    conn,
    category: str = None,
    dry_run: bool = False
) -> dict:
    """
    Main computation loop.
    For each plant with multiple EPDs, compute pairwise attribution
    between consecutive EPD versions.
    """
    stats = {
        "plants_processed":    0,
        "plants_skipped":      0,
        "attributions_computed": 0,
        "no_grid_data":        0,
        "errors":              0,
    }

    log.info("=" * 60)
    log.info("Computing GWP attribution")
    log.info("=" * 60)

    plants = get_plants_with_multiple_epds(conn, category)
    log.info(f"Found {len(plants)} plants with 2+ EPD versions")

    batch = []
    BATCH_SIZE = 200

    for plant in tqdm(plants, desc="Plants", unit="plant"):
        plant_id   = str(plant["plant_id"])
        subregion  = plant["egrid_subregion"]

        try:
            epd_versions = get_epd_versions_for_plant(conn, plant_id)

            if len(epd_versions) < 2:
                stats["plants_skipped"] += 1
                continue

            # Group by declared_unit — only compare consecutive years within same unit
            from collections import defaultdict
            by_unit = defaultdict(list)
            for epd in epd_versions:
                by_unit[epd["declared_unit"]].append(epd)
            consecutive_pairs = []
            for unit_epds in by_unit.values():
                unit_epds.sort(key=lambda e: e["epd_year"])
                for i in range(len(unit_epds) - 1):
                    consecutive_pairs.append((unit_epds[i], unit_epds[i+1]))
            if not consecutive_pairs:
                stats["plants_skipped"] += 1
                continue
            for v1, v2 in consecutive_pairs:
                period_start = v1["issued_at"]
                period_end   = v2["issued_at"]


                # Skip if already computed
                if not dry_run and attribution_already_computed(
                    conn, plant_id, period_start, period_end
                ):
                    continue

                gwp_start = v1["gwp_total"]
                gwp_end   = v2["gwp_total"]

                if gwp_start is None or gwp_end is None:
                    continue

                # Get grid carbon intensity for both years
                year_start = period_start.year
                year_end   = period_end.year

                # Skip grid lookup for international/unknown subregion plants
                if not subregion or subregion == "UNKNOWN":
                    grid_start = 0.0
                    grid_end   = 0.0
                else:
                    grid_start = get_grid_carbon_for_subregion_year(
                        conn, subregion, year_start
                    ) or 0.0
                    grid_end = get_grid_carbon_for_subregion_year(
                        conn, subregion, year_end
                    ) or 0.0
                if grid_start == 0.0 and grid_end == 0.0 and subregion:
                    stats["no_grid_data"] += 1
                attribution = compute_grid_contribution(
                    gwp_start, gwp_end,
                    grid_start, grid_end
                )

                record = {
                    "plant_id":      plant_id,
                    "period_start":  period_start,
                    "period_end":    period_end,
                    "gwp_start":     gwp_start,
                    "gwp_end":       gwp_end,
                    "declared_unit": v1.get("declared_unit"),
                    **attribution
                }

                batch.append(record)
                stats["attributions_computed"] += 1

                if len(batch) >= BATCH_SIZE:
                    insert_attribution_records(conn, batch, dry_run)
                    batch = []

            stats["plants_processed"] += 1

        except Exception as e:
            stats["errors"] += 1
            log.error(f"Error processing plant {plant_id}: {e}")
            continue

    # Insert remaining batch
    if batch:
        insert_attribution_records(conn, batch, dry_run)

    log.info(f"Attribution computation complete: {stats}")
    return stats


def print_insights(conn):
    """
    Print the first real insights from the attribution data.
    This is what you show Andrew.
    """
    log.info("\n" + "=" * 60)
    log.info("INSIGHTS — WHICH PLANTS ARE ACTUALLY DECARBONIZING?")
    log.info("=" * 60)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:

        # Top 10 most improved plants overall
        cur.execute("""
            SELECT
                p.name,
                p.state,
                p.material_category,
                a.gwp_start,
                a.gwp_end,
                a.pct_change_total,
                a.pct_from_grid,
                a.pct_from_process,
                a.period_start::date,
                a.period_end::date
            FROM gwp_attribution a
            JOIN plants p ON a.plant_id = p.id
            WHERE a.pct_change_total IS NOT NULL
              AND a.pct_change_total < 0
            ORDER BY a.pct_change_total ASC
            LIMIT 10;
        """)
        top_improvers = cur.fetchall()

        log.info("\nTop 10 most improved plants:")
        log.info(f"{'Plant':<40} {'State':<6} {'Category':<12} {'Change':>8} {'Grid%':>7} {'Process%':>10}")
        log.info("-" * 90)
        for r in top_improvers:
            log.info(
                f"{r['name'][:38]:<40} "
                f"{r['state']:<6} "
                f"{r['material_category']:<12} "
                f"{r['pct_change_total']:>+7.1f}% "
                f"{r['pct_from_grid']:>6.0f}% "
                f"{r['pct_from_process']:>9.0f}%"
            )

        # By category — what's driving improvement?
        cur.execute("""
            SELECT
                p.material_category,
                COUNT(*) AS plant_pairs,
                ROUND(AVG(a.pct_change_total)::numeric, 1) AS avg_pct_change,
                ROUND(AVG(a.pct_from_grid)::numeric, 1) AS avg_pct_grid,
                ROUND(AVG(a.pct_from_process)::numeric, 1) AS avg_pct_process
            FROM gwp_attribution a
            JOIN plants p ON a.plant_id = p.id
            WHERE a.pct_change_total IS NOT NULL
            GROUP BY p.material_category
            ORDER BY avg_pct_change ASC;
        """)
        by_category = cur.fetchall()

        log.info("\n\nBy material category — average change and what's driving it:")
        log.info(f"{'Category':<15} {'Pairs':>6} {'Avg Change':>11} {'% Grid':>8} {'% Process':>11}")
        log.info("-" * 60)
        for r in by_category:
            log.info(
                f"{r['material_category']:<15} "
                f"{r['plant_pairs']:>6} "
                f"{r['avg_pct_change']:>+10.1f}% "
                f"{r['avg_pct_grid']:>7.0f}% "
                f"{r['avg_pct_process']:>10.0f}%"
            )

        # The headline number
        cur.execute("""
            SELECT
                COUNT(DISTINCT plant_id) AS plants_improving,
                COUNT(DISTINCT plant_id) FILTER (
                    WHERE pct_from_process < pct_from_grid
                ) AS grid_driven,
                COUNT(DISTINCT plant_id) FILTER (
                    WHERE pct_from_process >= pct_from_grid
                ) AS process_driven
            FROM gwp_attribution
            WHERE pct_change_total < 0;
        """)
        headline = cur.fetchone()

        log.info(f"\n\nHEADLINE:")
        log.info(f"Plants with improving GWP:          {headline['plants_improving']}")
        log.info(f"  → Improvement grid-driven:        {headline['grid_driven']}")
        log.info(f"  → Improvement process-driven:     {headline['process_driven']}")
        log.info("\nThis is the finding. This is the story.")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Compute GWP attribution for all plants in TimescaleDB"
    )
    parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Compute attribution for a specific material category only"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute but don't write to database"
    )
    parser.add_argument(
        "--insights-only",
        action="store_true",
        help="Skip computation, just print insights from existing data"
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("EMBODIED CARBON OBSERVATORY — ATTRIBUTION COMPUTATION")
    log.info("=" * 60)

    conn = get_connection()

    try:
        if not args.insights_only:
            compute_all_attributions(conn, args.category, args.dry_run)

        if not args.dry_run:
            print_insights(conn)

    except KeyboardInterrupt:
        log.info("\nInterrupted. Committing partial progress...")
        conn.commit()
    except Exception as e:
        log.error(f"Fatal error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
