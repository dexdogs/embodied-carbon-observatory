"""
api.py
======
FastAPI layer for the Embodied Carbon Observatory.

Sits between TimescaleDB and the Next.js frontend.
Returns JSON that Mapbox, D3, and Cytoscape can consume directly.

Endpoints:
    GET /plants                     — all plants, filterable by category/state/radius
    GET /plants/:id                 — single plant detail
    GET /plants/:id/epd-history     — GWP over time for one plant
    GET /plants/:id/attribution     — grid vs process decomposition
    GET /plants/:id/chain           — upstream LCI dependency graph
    GET /plants/:id/compare         — compare to nearby alternatives
    GET /materials                  — list all material categories
    GET /search                     — search plants by name/manufacturer
    GET /insights                   — headline findings across all plants
    GET /health                     — health check

Usage:
    uvicorn api:app --reload --port 8000

Requirements:
    pip install fastapi uvicorn psycopg2-binary python-dotenv
"""

import os
import logging
from typing import Optional
from contextlib import asynccontextmanager

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ============================================================
# DATABASE POOL
# ============================================================

pool: psycopg2.pool.ThreadedConnectionPool = None


def get_pool():
    global pool
    if pool is None:
        pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=DATABASE_URL
        )
    return pool


def get_conn():
    return get_pool().getconn()


def release_conn(conn):
    get_pool().putconn(conn)


def query(sql: str, params=None) -> list[dict]:
    """Execute a query and return list of dicts."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        release_conn(conn)


def query_one(sql: str, params=None) -> Optional[dict]:
    """Execute a query and return single dict or None."""
    results = query(sql, params)
    return results[0] if results else None


# ============================================================
# APP SETUP
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize connection pool on startup."""
    log.info("Starting Embodied Carbon Observatory API...")
    get_pool()
    log.info("Database pool initialized.")
    yield
    log.info("Shutting down...")
    if pool:
        pool.closeall()


app = FastAPI(
    title="Embodied Carbon Observatory API",
    description="Temporal supply chain carbon data for US building materials",
    version="1.0.0",
    lifespan=lifespan
)

# Allow frontend on Vercel to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
def health():
    """Verify API and database are up."""
    try:
        result = query_one("SELECT COUNT(*) as plants FROM plants;")
        epds   = query_one("SELECT COUNT(*) as epds FROM epd_versions;")
        return {
            "status":  "ok",
            "plants":  result["plants"],
            "epds":    epds["epds"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# PLANTS
# ============================================================

@app.get("/plants")
def get_plants(
    lat:      Optional[float] = Query(None, description="User latitude"),
    lng:      Optional[float] = Query(None, description="User longitude"),
    radius_miles: float       = Query(500, description="Search radius in miles"),
    category: Optional[str]   = Query(None, description="Material category filter"),
    state:    Optional[str]   = Query(None, description="State filter e.g. MA"),
    search:   Optional[str]   = Query(None, description="Search by name or manufacturer"),
    limit:    int             = Query(500, le=2000),
):
    """
    Get plants as GeoJSON FeatureCollection.
    If lat/lng provided, returns plants within radius ordered by distance.
    Each feature includes GWP trend metadata for map dot sizing/coloring.
    """
    conditions = ["p.lat IS NOT NULL", "p.lng IS NOT NULL"]
    params     = []

    if category:
        conditions.append("p.material_category = %s")
        params.append(category.lower())

    if state:
        conditions.append("p.state = %s")
        params.append(state.upper())

    if search:
        conditions.append(
            "(LOWER(p.name) LIKE %s OR LOWER(p.manufacturer) LIKE %s)"
        )
        params.extend([f"%{search.lower()}%", f"%{search.lower()}%"])

    if lat and lng:
        radius_meters = radius_miles * 1609.34
        conditions.append(
            "ST_DWithin(p.location::geography, ST_MakePoint(%s, %s)::geography, %s)"
        )
        params.extend([lng, lat, radius_meters])

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Distance ordering if user location provided
    distance_select = ""
    distance_order  = "p.name"
    if lat and lng:
        distance_select = (
            f", ROUND((ST_Distance(p.location::geography, "
            f"ST_MakePoint({lng}, {lat})::geography) / 1609.34)::numeric, 1) "
            f"AS distance_miles"
        )
        distance_order = "distance_miles ASC"

    sql = f"""
        SELECT
            p.id::text,
            p.name,
            p.manufacturer,
            p.city,
            p.state,
            p.material_category,
            p.material_subcategory,
            p.lat,
            p.lng,
            p.egrid_subregion,
            -- Latest GWP from continuous aggregate
            gd.avg_gwp            AS latest_gwp,
            gd.period             AS latest_epd_period,
            -- Attribution summary
            a.pct_change_total    AS gwp_pct_change,
            a.pct_from_grid       AS pct_change_from_grid,
            a.pct_from_process    AS pct_change_from_process,
            a.attribution_confidence,
            -- Temporal flag: has 2+ years of EPD data
            CASE WHEN epd_years.year_count >= 2 THEN true ELSE false END AS has_temporal
            {distance_select}
        FROM plants p
        -- Join to most recent gwp_delta
        LEFT JOIN LATERAL (
            SELECT avg_gwp, period
            FROM gwp_deltas
            WHERE plant_id = p.id
            ORDER BY period DESC
            LIMIT 1
        ) gd ON TRUE
        -- Temporal year count
        LEFT JOIN LATERAL (
            SELECT COUNT(DISTINCT EXTRACT(year FROM issued_at)) AS year_count
            FROM epd_versions
            WHERE plant_id = p.id AND gwp_total IS NOT NULL
        ) epd_years ON TRUE
        -- Join to most recent attribution
        LEFT JOIN LATERAL (
            SELECT
                pct_change_total,
                pct_from_grid,
                pct_from_process,
                attribution_confidence
            FROM gwp_attribution
            WHERE plant_id = p.id
            ORDER BY period_end DESC
            LIMIT 1
        ) a ON TRUE
        {where}
        ORDER BY gd.avg_gwp IS NULL ASC, {distance_order}
        LIMIT %s;
    """
    params.append(limit)

    rows = query(sql, params)

    # Format as GeoJSON for Mapbox
    features = []
    for row in rows:
        features.append({
            "type": "Feature",
            "geometry": {
                "type":        "Point",
                "coordinates": [row["lng"], row["lat"]]
            },
            "properties": {
                k: v for k, v in row.items()
                if k not in ("lat", "lng")
            }
        })

    return {
        "type":     "FeatureCollection",
        "features": features,
        "count":    len(features)
    }


@app.get("/plants/{plant_id}")
def get_plant(plant_id: str):
    """Get full detail for a single plant."""
    sql = """
        SELECT
            p.id::text,
            p.name,
            p.manufacturer,
            p.address,
            p.city,
            p.state,
            p.zip,
            p.lat,
            p.lng,
            p.egrid_subregion,
            p.material_category,
            p.material_subcategory,
            p.ec3_plant_id,
            p.data_source,
            p.created_at,
            COUNT(DISTINCT e.id) AS epd_count,
            MIN(e.issued_at)     AS first_epd_date,
            MAX(e.issued_at)     AS latest_epd_date,
            MIN(e.gwp_total)     AS min_gwp_ever,
            MAX(e.gwp_total)     AS max_gwp_ever
        FROM plants p
        LEFT JOIN epd_versions e ON e.plant_id = p.id
        WHERE p.id = %s::uuid
        GROUP BY p.id;
    """
    plant = query_one(sql, (plant_id,))
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")
    return plant


# ============================================================
# EPD HISTORY — THE TIME SERIES
# ============================================================

@app.get("/plants/{plant_id}/epd-history")
def get_epd_history(plant_id: str):
    """
    GWP trajectory over time for a single plant.
    This is what powers the D3 time-series chart.
    Returns all EPD versions ordered chronologically.
    """
    # Verify plant exists
    plant = query_one(
        "SELECT id, name, material_category FROM plants WHERE id = %s::uuid",
        (plant_id,)
    )
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")

    sql = """
        SELECT
            e.id::text,
            e.ec3_epd_id,
            e.issued_at,
            e.expired_at,
            e.gwp_total,
            e.gwp_fossil,
            e.gwp_biogenic,
            e.gwp_luluc,
            e.declared_unit,
            e.is_facility_specific,
            e.is_product_specific,
            e.program_operator,
            e.epd_version,
            -- Grid carbon intensity at time of issue
            g.co2e_rate_lb_per_mwh AS grid_co2e_at_issue,
            (COALESCE(g.resource_mix_pct_wind,0) + COALESCE(g.resource_mix_pct_solar,0) + COALESCE(g.resource_mix_pct_hydro,0)) AS resource_mix_pct_renewable
        FROM epd_versions e
        LEFT JOIN plants p ON p.id = e.plant_id
        LEFT JOIN grid_carbon g ON (
            g.egrid_subregion = p.egrid_subregion
            AND EXTRACT(year FROM g.year) = EXTRACT(year FROM e.issued_at)
        )
        WHERE e.plant_id = %s::uuid
          AND e.gwp_total IS NOT NULL
        ORDER BY e.issued_at ASC;
    """
    versions = query(sql, (plant_id,))

    # Also get the industry baseline for context
    baseline_sql = """
        SELECT
            year,
            baseline_gwp,
            percentile_10,
            percentile_50,
            percentile_90
        FROM material_baselines
        WHERE material_category = %s
          AND region = 'national'
        ORDER BY year;
    """
    baselines = query(baseline_sql, (plant["material_category"],))

    return {
        "plant_id":        plant_id,
        "plant_name":      plant["name"],
        "material_category": plant["material_category"],
        "epd_versions":    versions,
        "industry_baselines": baselines,
        "version_count":   len(versions),
    }


# ============================================================
# ATTRIBUTION — THE CORE INSIGHT
# ============================================================

@app.get("/plants/{plant_id}/attribution")
def get_attribution(plant_id: str):
    """
    Grid vs. process attribution for a plant.
    Answers: "Was this plant's improvement real, or just a cleaner grid?"
    This powers the attribution breakdown panel in the frontend.
    """
    plant = query_one(
        "SELECT id, name, material_category, egrid_subregion FROM plants WHERE id = %s::uuid",
        (plant_id,)
    )
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")

    sql = """
        SELECT
            a.id::text,
            a.period_start,
            a.period_end,
            a.gwp_start,
            a.gwp_end,
            a.gwp_delta_total,
            a.grid_co2e_start,
            a.grid_co2e_end,
            a.grid_co2e_delta,
            a.gwp_delta_grid,
            a.gwp_delta_process,
            a.pct_change_total,
            a.pct_from_grid,
            a.pct_from_process,
            a.attribution_confidence,
            -- Human readable verdict
            CASE
                WHEN a.pct_change_total >= 0 THEN 'increasing'
                WHEN a.pct_from_process <= -50 THEN 'process_improvement'
                WHEN a.pct_from_grid <= -50 THEN 'grid_improvement'
                ELSE 'mixed'
            END AS verdict
        FROM gwp_attribution a
        WHERE a.plant_id = %s::uuid
        ORDER BY a.period_end ASC;
    """
    attributions = query(sql, (plant_id,))

    # Summary across all periods
    summary_sql = """
        SELECT
            ROUND(AVG(pct_change_total)::numeric, 1) AS avg_pct_change,
            ROUND(AVG(pct_from_grid)::numeric, 1)    AS avg_pct_from_grid,
            ROUND(AVG(pct_from_process)::numeric, 1) AS avg_pct_from_process,
            COUNT(*) AS periods_analyzed
        FROM gwp_attribution
        WHERE plant_id = %s::uuid;
    """
    summary = query_one(summary_sql, (plant_id,))

    return {
        "plant_id":     plant_id,
        "plant_name":   plant["name"],
        "subregion":    plant["egrid_subregion"],
        "attributions": attributions,
        "summary":      summary,
    }


# ============================================================
# SUPPLY CHAIN GRAPH — FOR CYTOSCAPE
# ============================================================

@app.get("/plants/{plant_id}/chain")
def get_chain(plant_id: str):
    """
    Upstream supply chain dependency graph for a plant.
    Returns nodes and edges formatted for Cytoscape.js.
    """
    plant = query_one(
        "SELECT id, name, material_category, lat, lng FROM plants WHERE id = %s::uuid",
        (plant_id,)
    )
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")

    # Get linked LCI nodes
    nodes_sql = """
        SELECT
            n.id::text,
            n.name,
            n.type,
            n.material_category,
            n.lat,
            n.lng,
            n.region,
            n.region_confidence,
            n.gwp_per_unit,
            n.unit,
            l.confidence AS link_confidence,
            l.match_method
        FROM plant_lci_links l
        JOIN lci_nodes n ON n.id = l.lci_node_id
        WHERE l.plant_id = %s::uuid;
    """
    lci_nodes = query(nodes_sql, (plant_id,))

    # Get edges between linked nodes
    node_ids = [n["id"] for n in lci_nodes]

    edges = []
    if node_ids:
        placeholders = ",".join(["%s::uuid"] * len(node_ids))
        edges_sql = f"""
            SELECT
                e.id::text,
                e.source_node_id::text,
                e.target_node_id::text,
                e.flow_name,
                e.amount,
                e.unit
            FROM lci_edges e
            WHERE e.source_node_id IN ({placeholders})
               OR e.target_node_id IN ({placeholders});
        """
        edges = query(edges_sql, node_ids * 2)

    # Format for Cytoscape
    # Plant itself is the root node
    cytoscape_nodes = [
        {
            "data": {
                "id":       plant_id,
                "label":    plant["name"],
                "type":     "manufacturer",
                "lat":      plant["lat"],
                "lng":      plant["lng"],
                "is_root":  True,
            }
        }
    ]

    for n in lci_nodes:
        cytoscape_nodes.append({
            "data": {
                "id":         n["id"],
                "label":      n["name"],
                "type":       n["type"],
                "lat":        n["lat"],
                "lng":        n["lng"],
                "region":     n["region"],
                "confidence": n["link_confidence"],
                "gwp":        n["gwp_per_unit"],
            }
        })

    cytoscape_edges = []
    for e in edges:
        cytoscape_edges.append({
            "data": {
                "id":        e["id"],
                "source":    e["source_node_id"],
                "target":    e["target_node_id"],
                "label":     e["flow_name"],
                "amount":    e["amount"],
                "unit":      e["unit"],
            }
        })

    # Add edge from each LCI node to plant
    for n in lci_nodes:
        cytoscape_edges.append({
            "data": {
                "id":     f"link-{n['id']}-{plant_id}",
                "source": n["id"],
                "target": plant_id,
                "label":  "supplies",
            }
        })

    return {
        "plant_id": plant_id,
        "elements": {
            "nodes": cytoscape_nodes,
            "edges": cytoscape_edges,
        },
        "node_count": len(cytoscape_nodes),
        "edge_count": len(cytoscape_edges),
    }


# ============================================================
# COMPARISON — NEARBY ALTERNATIVES
# ============================================================

@app.get("/plants/{plant_id}/compare")
def get_comparison(
    plant_id:     str,
    radius_miles: float = Query(200, description="Radius to search for alternatives"),
    limit:        int   = Query(5, le=20),
):
    """
    Find nearby plants in the same material category
    and compare their GWP trajectories.
    This is the "find a lower-carbon local alternative" feature.
    """
    plant = query_one(
        "SELECT id, name, material_category, lat, lng FROM plants WHERE id = %s::uuid",
        (plant_id,)
    )
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")

    radius_meters = radius_miles * 1609.34

    sql = """
        SELECT
            p.id::text,
            p.name,
            p.state,
            p.material_category,
            p.lat,
            p.lng,
            ROUND((ST_Distance(
                p.location::geography,
                ST_MakePoint(%s, %s)::geography
            ) / 1609.34)::numeric, 1) AS distance_miles,
            gd.avg_gwp AS latest_gwp,
            a.pct_change_total AS gwp_trend_pct
        FROM plants p
        LEFT JOIN LATERAL (
            SELECT avg_gwp FROM gwp_deltas
            WHERE plant_id = p.id
            ORDER BY period DESC LIMIT 1
        ) gd ON TRUE
        LEFT JOIN LATERAL (
            SELECT pct_change_total FROM gwp_attribution
            WHERE plant_id = p.id
            ORDER BY period_end DESC LIMIT 1
        ) a ON TRUE
        WHERE p.material_category = %s
          AND p.id != %s::uuid
          AND ST_DWithin(
              p.location::geography,
              ST_MakePoint(%s, %s)::geography,
              %s
          )
          AND gd.avg_gwp IS NOT NULL
        ORDER BY gd.avg_gwp ASC
        LIMIT %s;
    """

    alternatives = query(sql, (
        plant["lng"], plant["lat"],
        plant["material_category"],
        plant_id,
        plant["lng"], plant["lat"],
        radius_meters,
        limit
    ))

    # Get the reference plant's current GWP
    ref_gwp_sql = """
        SELECT avg_gwp FROM gwp_deltas
        WHERE plant_id = %s::uuid
        ORDER BY period DESC LIMIT 1;
    """
    ref_gwp = query_one(ref_gwp_sql, (plant_id,))

    return {
        "reference_plant": {
            "id":         plant_id,
            "name":       plant["name"],
            "latest_gwp": ref_gwp["avg_gwp"] if ref_gwp else None,
        },
        "alternatives":    alternatives,
        "radius_miles":    radius_miles,
        "category":        plant["material_category"],
    }


# ============================================================
# SEARCH
# ============================================================

@app.get("/search")
def search(
    q:    str   = Query(..., min_length=2, description="Search term"),
    limit: int  = Query(20, le=100),
):
    """
    Search plants by name or manufacturer.
    Returns lightweight results for the search dropdown.
    """
    sql = """
        SELECT
            p.id::text,
            p.name,
            p.manufacturer,
            p.city,
            p.state,
            p.material_category,
            p.lat,
            p.lng,
            gd.avg_gwp AS latest_gwp
        FROM plants p
        LEFT JOIN LATERAL (
            SELECT avg_gwp FROM gwp_deltas
            WHERE plant_id = p.id
            ORDER BY period DESC LIMIT 1
        ) gd ON TRUE
        WHERE
            LOWER(p.name)         LIKE %s
            OR LOWER(p.manufacturer) LIKE %s
        ORDER BY
            CASE WHEN LOWER(p.name) LIKE %s THEN 0 ELSE 1 END,
            p.name
        LIMIT %s;
    """
    term    = f"%{q.lower()}%"
    exact   = f"{q.lower()}%"
    results = query(sql, (term, term, exact, limit))

    return {
        "query":   q,
        "results": results,
        "count":   len(results),
    }


# ============================================================
# MATERIALS LIST
# ============================================================

@app.get("/materials")
def get_materials():
    """
    List all material categories with plant and EPD counts.
    Used to populate the filter UI.
    """
    sql = """
        SELECT
            p.material_category,
            COUNT(DISTINCT p.id)  AS plant_count,
            COUNT(DISTINCT e.id)  AS epd_count,
            ROUND(AVG(e.gwp_total)::numeric, 2) AS avg_gwp
        FROM plants p
        LEFT JOIN epd_versions e ON e.plant_id = p.id
        GROUP BY p.material_category
        ORDER BY plant_count DESC;
    """
    return query(sql)


# ============================================================
# INSIGHTS — HEADLINE FINDINGS
# ============================================================

@app.get("/insights")
def get_insights(
    category: Optional[str] = Query(None),
    state:    Optional[str] = Query(None),
):
    """
    Headline findings across all plants.
    Powers the summary stats panel on the map.
    """
    conditions = []
    params     = []

    if category:
        conditions.append("p.material_category = %s")
        params.append(category.lower())
    if state:
        conditions.append("p.state = %s")
        params.append(state.upper())

    where = "AND " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT
            COUNT(DISTINCT a.plant_id) AS plants_with_attribution,
            COUNT(DISTINCT a.plant_id) FILTER (
                WHERE a.pct_change_total < 0
            ) AS plants_improving,
            COUNT(DISTINCT a.plant_id) FILTER (
                WHERE a.pct_change_total > 0
            ) AS plants_worsening,
            COUNT(DISTINCT a.plant_id) FILTER (
                WHERE a.pct_change_total < 0
                  AND a.pct_from_process < a.pct_from_grid
            ) AS improvement_process_driven,
            COUNT(DISTINCT a.plant_id) FILTER (
                WHERE a.pct_change_total < 0
                  AND a.pct_from_grid <= a.pct_from_process
            ) AS improvement_grid_driven,
            ROUND(AVG(a.pct_change_total)::numeric, 1) AS avg_gwp_change_pct
        FROM gwp_attribution a
        JOIN plants p ON a.plant_id = p.id
        WHERE a.pct_change_total IS NOT NULL
        {where};
    """
    summary = query_one(sql, params)

    # Trend by year across all plants
    trend_sql = f"""
        SELECT
            EXTRACT(year FROM gd.period)::int AS year,
            ROUND(AVG(gd.avg_gwp)::numeric, 2) AS avg_gwp,
            COUNT(DISTINCT gd.plant_id) AS plant_count
        FROM gwp_deltas gd
        JOIN plants p ON p.id = gd.plant_id
        WHERE gd.avg_gwp IS NOT NULL
        {where}
        GROUP BY year
        ORDER BY year;
    """
    trend = query(trend_sql, params)

    return {
        "summary": summary,
        "trend_by_year": trend,
        "filters": {
            "category": category,
            "state":    state,
        }
    }


# ============================================================
# GRID CARBON HISTORY
# ============================================================

@app.get("/grid/{subregion}")
def get_grid_history(subregion: str):
    """
    Grid carbon intensity history for a subregion.
    Used to show the grid context alongside plant GWP.
    """
    sql = """
        SELECT
            EXTRACT(year FROM year)::int AS year,
            co2e_rate_lb_per_mwh,
            resource_mix_pct_coal,
            resource_mix_pct_gas,
            resource_mix_pct_nuclear,
            resource_mix_pct_wind,
            resource_mix_pct_solar,
            resource_mix_pct_hydro
        FROM grid_carbon
        WHERE egrid_subregion = %s
        ORDER BY year ASC;
    """
    records = query(sql, (subregion.upper(),))
    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"No grid data found for subregion: {subregion}"
        )

    return {
        "subregion": subregion.upper(),
        "history":   records,
    }
# reorder Wed Apr 15 13:32:15 UTC 2026
