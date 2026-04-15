import os
import logging
import re
from typing import Optional
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

pg_pool: asyncpg.Pool = None

async def get_pool() -> asyncpg.Pool:
    global pg_pool
    if pg_pool is None:
        pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return pg_pool

async def query(sql: str, params=None) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *(params or []))
        return [dict(row) for row in rows]

async def query_one(sql: str, params=None) -> Optional[dict]:
    results = await query(sql, params)
    return results[0] if results else None

def pg(sql: str) -> str:
    """Convert %s placeholders to $1, $2, etc for asyncpg."""
    i = 0
    def replace(m):
        nonlocal i
        i += 1
        return f'${i}'
    return re.sub(r'%s', replace, sql)

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting Embodied Carbon Observatory API...")
    await get_pool()
    log.info("Database pool initialized.")
    yield
    if pg_pool:
        await pg_pool.close()

app = FastAPI(title="Embodied Carbon Observatory API", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["GET"], allow_headers=["*"])

@app.get("/health")
async def health():
    try:
        result = await query_one("SELECT COUNT(*) as plants FROM plants;")
        epds = await query_one("SELECT COUNT(*) as epds FROM epd_versions;")
        return {"status": "ok", "plants": result["plants"], "epds": epds["epds"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/plants")
async def get_plants(
    lat: Optional[float] = Query(None),
    lng: Optional[float] = Query(None),
    radius_miles: float = Query(500),
    category: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(500, le=2000),
):
    conditions = ["p.lat IS NOT NULL", "p.lng IS NOT NULL"]
    params = []

    if category:
        params.append(category.lower())
        conditions.append(f"p.material_category = ${len(params)}")
    if state:
        params.append(state.upper())
        conditions.append(f"p.state = ${len(params)}")
    if search:
        params.append(f"%{search.lower()}%")
        params.append(f"%{search.lower()}%")
        conditions.append(f"(LOWER(p.name) LIKE ${len(params)-1} OR LOWER(p.manufacturer) LIKE ${len(params)})")
    if lat and lng:
        radius_meters = radius_miles * 1609.34
        params.extend([lng, lat, radius_meters])
        conditions.append(f"ST_DWithin(p.location::geography, ST_MakePoint(${len(params)-2}, ${len(params)-1})::geography, ${len(params)})")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    distance_select = ""
    distance_order = "p.name"
    if lat and lng:
        distance_select = f", ROUND((ST_Distance(p.location::geography, ST_MakePoint({lng}, {lat})::geography) / 1609.34)::numeric, 1) AS distance_miles"
        distance_order = "distance_miles ASC"

    params.append(limit)
    sql = f"""
        SELECT
            p.id::text, p.name, p.manufacturer, p.city, p.state,
            p.material_category, p.material_subcategory, p.lat, p.lng,
            p.egrid_subregion,
            gd.avg_gwp AS latest_gwp,
            gd.period AS latest_epd_period,
            a.pct_change_total AS gwp_pct_change,
            a.pct_from_grid AS pct_change_from_grid,
            a.pct_from_process AS pct_change_from_process,
            a.attribution_confidence,
            CASE WHEN epd_years.year_count >= 2 THEN true ELSE false END AS has_temporal
            {distance_select}
        FROM plants p
        LEFT JOIN LATERAL (
            SELECT avg_gwp, period FROM gwp_deltas
            WHERE plant_id = p.id ORDER BY period DESC LIMIT 1
        ) gd ON TRUE
        LEFT JOIN LATERAL (
            SELECT pct_change_total, pct_from_grid, pct_from_process, attribution_confidence
            FROM gwp_attribution WHERE plant_id = p.id ORDER BY period_end DESC LIMIT 1
        ) a ON TRUE
        LEFT JOIN LATERAL (
            SELECT COUNT(DISTINCT EXTRACT(year FROM issued_at)) AS year_count
            FROM epd_versions WHERE plant_id = p.id AND gwp_total IS NOT NULL
        ) epd_years ON TRUE
        {where}
        ORDER BY gd.avg_gwp IS NULL ASC, {distance_order}
        LIMIT ${len(params)};
    """

    rows = await query(sql, params)
    features = []
    for row in rows:
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [row["lng"], row["lat"]]},
            "properties": {k: v for k, v in row.items() if k not in ("lat", "lng")}
        })
    return {"type": "FeatureCollection", "features": features, "count": len(features)}

@app.get("/plants/{plant_id}")
async def get_plant(plant_id: str):
    sql = """
        SELECT p.id::text, p.name, p.manufacturer, p.address, p.city, p.state,
            p.zip, p.lat, p.lng, p.egrid_subregion, p.material_category,
            p.material_subcategory, p.ec3_plant_id, p.data_source, p.created_at,
            COUNT(DISTINCT e.id) AS epd_count,
            MIN(e.issued_at) AS first_epd_date, MAX(e.issued_at) AS latest_epd_date,
            MIN(e.gwp_total) AS min_gwp_ever, MAX(e.gwp_total) AS max_gwp_ever
        FROM plants p LEFT JOIN epd_versions e ON e.plant_id = p.id
        WHERE p.id = $1::uuid GROUP BY p.id;
    """
    plant = await query_one(sql, (plant_id,))
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")
    return plant

@app.get("/plants/{plant_id}/epd-history")
async def get_epd_history(plant_id: str):
    plant = await query_one("SELECT id, name, material_category FROM plants WHERE id = $1::uuid", (plant_id,))
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")

    sql = """
        SELECT e.id::text, e.ec3_epd_id, e.issued_at, e.expired_at,
            e.gwp_total, e.gwp_fossil, e.gwp_biogenic, e.gwp_luluc,
            e.declared_unit, e.is_facility_specific, e.is_product_specific,
            e.program_operator, e.epd_version,
            g.co2e_rate_lb_per_mwh AS grid_co2e_at_issue,
            (COALESCE(g.resource_mix_pct_wind,0) + COALESCE(g.resource_mix_pct_solar,0) + COALESCE(g.resource_mix_pct_hydro,0)) AS resource_mix_pct_renewable
        FROM epd_versions e
        LEFT JOIN plants p ON p.id = e.plant_id
        LEFT JOIN grid_carbon g ON (
            g.egrid_subregion = p.egrid_subregion
            AND EXTRACT(year FROM g.year) = EXTRACT(year FROM e.issued_at)
        )
        WHERE e.plant_id = $1::uuid AND e.gwp_total IS NOT NULL
        ORDER BY e.issued_at ASC;
    """
    versions = await query(sql, (plant_id,))
    baselines = await query(
        "SELECT year, baseline_gwp, percentile_10, percentile_50, percentile_90 FROM material_baselines WHERE material_category = $1 AND region = 'national' ORDER BY year;",
        (plant["material_category"],)
    )
    return {"plant_id": plant_id, "plant_name": plant["name"], "material_category": plant["material_category"], "epd_versions": versions, "industry_baselines": baselines, "version_count": len(versions)}

@app.get("/plants/{plant_id}/attribution")
async def get_attribution(plant_id: str):
    plant = await query_one("SELECT id, name, material_category, egrid_subregion FROM plants WHERE id = $1::uuid", (plant_id,))
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")

    sql = """
        SELECT a.id::text, a.period_start, a.period_end,
            a.gwp_start, a.gwp_end, a.gwp_delta_total,
            a.grid_co2e_start, a.grid_co2e_end, a.grid_co2e_delta,
            a.gwp_delta_grid, a.gwp_delta_process,
            a.pct_change_total, a.pct_from_grid, a.pct_from_process,
            a.attribution_confidence,
            CASE
                WHEN a.pct_change_total >= 0 THEN 'increasing'
                WHEN a.pct_from_process <= -50 THEN 'process_improvement'
                WHEN a.pct_from_grid <= -50 THEN 'grid_improvement'
                ELSE 'mixed'
            END AS verdict
        FROM gwp_attribution a WHERE a.plant_id = $1::uuid ORDER BY a.period_end ASC;
    """
    attributions = await query(sql, (plant_id,))
    summary = await query_one("""
        SELECT ROUND(AVG(pct_change_total)::numeric,1) AS avg_pct_change,
               ROUND(AVG(pct_from_grid)::numeric,1) AS avg_pct_from_grid,
               ROUND(AVG(pct_from_process)::numeric,1) AS avg_pct_from_process,
               COUNT(*) AS periods_analyzed
        FROM gwp_attribution WHERE plant_id = $1::uuid;
    """, (plant_id,))
    return {"plant_id": plant_id, "plant_name": plant["name"], "subregion": plant["egrid_subregion"], "attributions": attributions, "summary": summary}

@app.get("/plants/{plant_id}/compare")
async def get_comparison(plant_id: str, radius_miles: float = Query(200), limit: int = Query(5, le=20)):
    plant = await query_one("SELECT id, name, material_category, lat, lng FROM plants WHERE id = $1::uuid", (plant_id,))
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")
    radius_meters = radius_miles * 1609.34
    sql = """
        SELECT p.id::text, p.name, p.state, p.material_category, p.lat, p.lng,
            ROUND((ST_Distance(p.location::geography, ST_MakePoint($1, $2)::geography) / 1609.34)::numeric, 1) AS distance_miles,
            gd.avg_gwp AS latest_gwp, a.pct_change_total AS gwp_trend_pct
        FROM plants p
        LEFT JOIN LATERAL (SELECT avg_gwp FROM gwp_deltas WHERE plant_id = p.id ORDER BY period DESC LIMIT 1) gd ON TRUE
        LEFT JOIN LATERAL (SELECT pct_change_total FROM gwp_attribution WHERE plant_id = p.id ORDER BY period_end DESC LIMIT 1) a ON TRUE
        WHERE p.material_category = $3 AND p.id != $4::uuid
          AND ST_DWithin(p.location::geography, ST_MakePoint($1, $2)::geography, $5)
          AND gd.avg_gwp IS NOT NULL
        ORDER BY gd.avg_gwp ASC LIMIT $6;
    """
    alternatives = await query(sql, (plant["lng"], plant["lat"], plant["material_category"], plant_id, radius_meters, limit))
    ref_gwp = await query_one("SELECT avg_gwp FROM gwp_deltas WHERE plant_id = $1::uuid ORDER BY period DESC LIMIT 1;", (plant_id,))
    return {"reference_plant": {"id": plant_id, "name": plant["name"], "latest_gwp": ref_gwp["avg_gwp"] if ref_gwp else None}, "alternatives": alternatives, "radius_miles": radius_miles, "category": plant["material_category"]}

@app.get("/plants/{plant_id}/chain")
async def get_chain(plant_id: str):
    plant = await query_one("SELECT id, name, material_category, lat, lng FROM plants WHERE id = $1::uuid", (plant_id,))
    if not plant:
        raise HTTPException(status_code=404, detail="Plant not found")
    return {"plant_id": plant_id, "elements": {"nodes": [{"data": {"id": plant_id, "label": plant["name"], "type": "manufacturer", "is_root": True}}], "edges": []}, "node_count": 1, "edge_count": 0}

@app.get("/search")
async def search(q: str = Query(..., min_length=2), limit: int = Query(20, le=100)):
    term = f"%{q.lower()}%"
    exact = f"{q.lower()}%"
    sql = """
        SELECT p.id::text, p.name, p.manufacturer, p.city, p.state,
            p.material_category, p.lat, p.lng, gd.avg_gwp AS latest_gwp
        FROM plants p
        LEFT JOIN LATERAL (SELECT avg_gwp FROM gwp_deltas WHERE plant_id = p.id ORDER BY period DESC LIMIT 1) gd ON TRUE
        WHERE LOWER(p.name) LIKE $1 OR LOWER(p.manufacturer) LIKE $1
        ORDER BY CASE WHEN LOWER(p.name) LIKE $2 THEN 0 ELSE 1 END, p.name
        LIMIT $3;
    """
    results = await query(sql, (term, exact, limit))
    return {"query": q, "results": results, "count": len(results)}

@app.get("/materials")
async def get_materials():
    return await query("""
        SELECT p.material_category, COUNT(DISTINCT p.id) AS plant_count,
               COUNT(DISTINCT e.id) AS epd_count, ROUND(AVG(e.gwp_total)::numeric,2) AS avg_gwp
        FROM plants p LEFT JOIN epd_versions e ON e.plant_id = p.id
        GROUP BY p.material_category ORDER BY plant_count DESC;
    """)

@app.get("/insights")
async def get_insights(category: Optional[str] = Query(None), state: Optional[str] = Query(None)):
    conditions, params = [], []
    if category:
        params.append(category.lower())
        conditions.append(f"p.material_category = ${len(params)}")
    if state:
        params.append(state.upper())
        conditions.append(f"p.state = ${len(params)}")
    where = "AND " + " AND ".join(conditions) if conditions else ""
    summary = await query_one(f"""
        SELECT COUNT(DISTINCT a.plant_id) AS plants_with_attribution,
               COUNT(DISTINCT a.plant_id) FILTER (WHERE a.pct_change_total < 0) AS plants_improving,
               COUNT(DISTINCT a.plant_id) FILTER (WHERE a.pct_change_total > 0) AS plants_worsening,
               ROUND(AVG(a.pct_change_total)::numeric,1) AS avg_gwp_change_pct
        FROM gwp_attribution a JOIN plants p ON a.plant_id = p.id
        WHERE a.pct_change_total IS NOT NULL {where};
    """, params)
    return {"summary": summary, "filters": {"category": category, "state": state}}

@app.get("/grid/{subregion}")
async def get_grid_history(subregion: str):
    records = await query("""
        SELECT EXTRACT(year FROM year)::int AS year, co2e_rate_lb_per_mwh,
               resource_mix_pct_coal, resource_mix_pct_gas, resource_mix_pct_nuclear,
               resource_mix_pct_wind, resource_mix_pct_solar, resource_mix_pct_hydro
        FROM grid_carbon WHERE egrid_subregion = $1 ORDER BY year ASC;
    """, (subregion.upper(),))
    if not records:
        raise HTTPException(status_code=404, detail=f"No grid data for: {subregion}")
    return {"subregion": subregion.upper(), "history": records}
