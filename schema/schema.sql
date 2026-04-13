-- ============================================================
-- EMBODIED CARBON OBSERVATORY
-- TimescaleDB Schema
-- All data sources: EC3 API, EPA eGRID, Federal LCA Commons
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ============================================================
-- TABLE 1: plants
-- One row per physical US manufacturing facility
-- Source: EC3 API /materials/plants/public
-- ============================================================

CREATE TABLE plants (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ec3_plant_id          TEXT UNIQUE NOT NULL,
    name                  TEXT NOT NULL,
    manufacturer          TEXT,
    address               TEXT,
    city                  TEXT,
    state                 CHAR(2),
    zip                   TEXT,
    lat                   DOUBLE PRECISION,
    lng                   DOUBLE PRECISION,
    location              GEOMETRY(Point, 4326),    -- PostGIS point for spatial queries
    egrid_subregion       TEXT,                     -- e.g. NPCC, SERC, WECC — joins to grid_carbon
    material_category     TEXT NOT NULL,            -- concrete, steel, timber, insulation, etc.
    material_subcategory  TEXT,                     -- ready-mix, structural steel, CLT, polyiso, etc.
    data_source           TEXT DEFAULT 'ec3',
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Spatial index for geo queries (e.g. plants within 200 miles of Boston)
CREATE INDEX idx_plants_location
    ON plants USING GIST (location);

-- Index for material category filtering
CREATE INDEX idx_plants_material_category
    ON plants (material_category);

-- Index for state filtering
CREATE INDEX idx_plants_state
    ON plants (state);

-- Index for eGRID subregion joining
CREATE INDEX idx_plants_egrid_subregion
    ON plants (egrid_subregion);


-- ============================================================
-- TABLE 2: epd_versions  [HYPERTABLE]
-- One row per EPD version per plant
-- This is the core time-series table
-- Source: EC3 API
-- Time dimension: issued_at
-- ============================================================

CREATE TABLE epd_versions (
    id                    UUID DEFAULT gen_random_uuid(),
    plant_id              UUID NOT NULL REFERENCES plants(id),
    ec3_epd_id            TEXT NOT NULL,
    issued_at             TIMESTAMPTZ NOT NULL,    -- TIME DIMENSION — hypertable partitions on this
    expired_at            TIMESTAMPTZ,             -- typically issued_at + 5 years
    gwp_total             DOUBLE PRECISION,        -- kg CO2e per declared unit — the key signal
    gwp_fossil            DOUBLE PRECISION,
    gwp_biogenic          DOUBLE PRECISION,
    gwp_luluc             DOUBLE PRECISION,
    declared_unit         TEXT,                    -- per m3, per tonne, per m2, etc.
    declared_unit_qty     DOUBLE PRECISION,
    is_facility_specific  BOOLEAN DEFAULT FALSE,   -- true = higher quality data
    is_product_specific   BOOLEAN DEFAULT FALSE,
    pcr_id                TEXT,                    -- product category rules governing this EPD
    program_operator      TEXT,                    -- NRMCA, AISC, AWC, etc.
    epd_version           INTEGER,                 -- version number within plant history
    data_source           TEXT DEFAULT 'ec3',
    raw_json              JSONB,                   -- store full EC3 response for reprocessing
    PRIMARY KEY (id, issued_at)
);

-- Convert to hypertable partitioned by issued_at
-- chunk_time_interval = 1 year is appropriate for EPD cadence
SELECT create_hypertable(
    'epd_versions',
    'issued_at',
    chunk_time_interval => INTERVAL '1 year'
);

-- Index for plant lookups over time
CREATE INDEX idx_epd_versions_plant_id
    ON epd_versions (plant_id, issued_at DESC);

-- Index for material querying (via join to plants)
CREATE INDEX idx_epd_versions_ec3_id
    ON epd_versions (ec3_epd_id);


-- ============================================================
-- TABLE 3: grid_carbon  [HYPERTABLE]
-- Annual EPA eGRID data by subregion
-- Source: EPA eGRID historical data 1996-2023
-- Time dimension: year (stored as TIMESTAMPTZ Jan 1 of each year)
-- ============================================================

CREATE TABLE grid_carbon (
    id                        UUID DEFAULT gen_random_uuid(),
    egrid_subregion           TEXT NOT NULL,         -- e.g. NPCC, SERC, WECC, ERCOT
    state                     CHAR(2),               -- primary state for this subregion
    year                      TIMESTAMPTZ NOT NULL,  -- TIME DIMENSION — Jan 1 of each year
    co2_rate_lb_per_mwh       DOUBLE PRECISION,      -- CO2 emission rate
    ch4_rate_lb_per_mwh       DOUBLE PRECISION,
    n2o_rate_lb_per_mwh       DOUBLE PRECISION,
    co2e_rate_lb_per_mwh      DOUBLE PRECISION NOT NULL, -- combined — used for attribution
    resource_mix_pct_coal     DOUBLE PRECISION,
    resource_mix_pct_gas      DOUBLE PRECISION,
    resource_mix_pct_nuclear  DOUBLE PRECISION,
    resource_mix_pct_hydro    DOUBLE PRECISION,
    resource_mix_pct_wind     DOUBLE PRECISION,
    resource_mix_pct_solar    DOUBLE PRECISION,
    resource_mix_pct_other    DOUBLE PRECISION,
    egrid_version             TEXT,                  -- e.g. eGRID2023
    data_source               TEXT DEFAULT 'epa_egrid',
    PRIMARY KEY (id, year)
);

-- Convert to hypertable partitioned by year
SELECT create_hypertable(
    'grid_carbon',
    'year',
    chunk_time_interval => INTERVAL '5 years'
);

-- Index for subregion + year lookups (the join key)
CREATE INDEX idx_grid_carbon_subregion_year
    ON grid_carbon (egrid_subregion, year DESC);


-- ============================================================
-- TABLE 4: gwp_deltas  [CONTINUOUS AGGREGATE]
-- Pre-computed annual GWP trends per plant per material category
-- Powers the "is this plant decarbonizing?" query instantly
-- Refreshes automatically as new epd_versions are ingested
-- ============================================================

CREATE MATERIALIZED VIEW gwp_deltas
WITH (timescaledb.continuous) AS
SELECT
    plant_id,
    time_bucket('1 year', issued_at)    AS period,
    COUNT(*)                             AS epd_count,
    AVG(gwp_total)                       AS avg_gwp,
    MIN(gwp_total)                       AS min_gwp,
    MAX(gwp_total)                       AS max_gwp,
    AVG(gwp_fossil)                      AS avg_gwp_fossil,
    AVG(gwp_biogenic)                    AS avg_gwp_biogenic,
    BOOL_OR(is_facility_specific)        AS any_facility_specific
FROM epd_versions
WHERE gwp_total IS NOT NULL
GROUP BY plant_id, time_bucket('1 year', issued_at)
WITH NO DATA;

-- Set refresh policy: refresh last 2 years of data every day
SELECT add_continuous_aggregate_policy('gwp_deltas',
    start_offset => INTERVAL '2 years',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day'
);


-- ============================================================
-- TABLE 5: gwp_attribution
-- The core analytical table — computed per plant per EPD transition
-- Decomposes GWP change into: grid-driven vs. process-driven
-- Built by joining epd_versions + grid_carbon + plants
-- ============================================================

CREATE TABLE gwp_attribution (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plant_id                UUID NOT NULL REFERENCES plants(id),
    period_start            TIMESTAMPTZ NOT NULL,   -- issued_at of earlier EPD version
    period_end              TIMESTAMPTZ NOT NULL,   -- issued_at of later EPD version
    gwp_start               DOUBLE PRECISION,       -- GWP at period_start
    gwp_end                 DOUBLE PRECISION,       -- GWP at period_end
    gwp_delta_total         DOUBLE PRECISION,       -- gwp_end - gwp_start (negative = improvement)
    grid_co2e_start         DOUBLE PRECISION,       -- eGRID co2e rate at period_start
    grid_co2e_end           DOUBLE PRECISION,       -- eGRID co2e rate at period_end
    grid_co2e_delta         DOUBLE PRECISION,       -- change in grid intensity
    gwp_delta_grid          DOUBLE PRECISION,       -- portion of GWP change attributable to grid
    gwp_delta_process       DOUBLE PRECISION,       -- remainder — attributable to manufacturing
    pct_change_total        DOUBLE PRECISION,       -- % change in GWP total
    pct_from_grid           DOUBLE PRECISION,       -- % of change explained by grid
    pct_from_process        DOUBLE PRECISION,       -- % of change explained by process
    attribution_confidence  TEXT DEFAULT 'medium',  -- high / medium / low
    computed_at             TIMESTAMPTZ DEFAULT NOW()
);

-- Index for plant lookups
CREATE INDEX idx_gwp_attribution_plant_id
    ON gwp_attribution (plant_id, period_end DESC);

-- Index for finding biggest improvers
CREATE INDEX idx_gwp_attribution_delta
    ON gwp_attribution (gwp_delta_total);


-- ============================================================
-- TABLE 6: material_baselines
-- CLF national and regional GWP baselines by material + year
-- Used to benchmark: is this plant above or below industry average?
-- Source: Carbon Leadership Forum Material Baselines 2023
-- ============================================================

CREATE TABLE material_baselines (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    material_category     TEXT NOT NULL,
    material_subcategory  TEXT,
    region                TEXT DEFAULT 'national',  -- national or state/regional
    year                  INTEGER NOT NULL,
    baseline_gwp          DOUBLE PRECISION,         -- industry average
    percentile_10         DOUBLE PRECISION,         -- best performers
    percentile_50         DOUBLE PRECISION,         -- median
    percentile_90         DOUBLE PRECISION,         -- worst performers
    declared_unit         TEXT,
    source                TEXT,                     -- e.g. CLF 2023 Baselines
    notes                 TEXT
);

CREATE INDEX idx_material_baselines_category_year
    ON material_baselines (material_category, year);


-- ============================================================
-- TABLE 7: lci_nodes
-- Process nodes from Federal LCA Commons
-- Upstream supply chain steps: quarry, mill, kiln, etc.
-- Source: FLCAC (USFS, NIST, NREL repos)
-- Static — not time-series
-- ============================================================

CREATE TABLE lci_nodes (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flcac_uuid          TEXT UNIQUE,                -- source UUID from FLCAC
    name                TEXT NOT NULL,              -- e.g. "limestone quarrying, US"
    type                TEXT NOT NULL,              -- raw_material / processing / manufacturing / transport / energy
    material_category   TEXT,
    lat                 DOUBLE PRECISION,
    lng                 DOUBLE PRECISION,
    location            GEOMETRY(Point, 4326),
    region              TEXT,                       -- e.g. US-ME, US-Southeast
    region_confidence   TEXT DEFAULT 'medium',      -- high / medium / low
    gwp_per_unit        DOUBLE PRECISION,           -- process-level GWP if available
    unit                TEXT,
    data_source         TEXT DEFAULT 'flcac',
    notes               TEXT
);

CREATE INDEX idx_lci_nodes_location
    ON lci_nodes USING GIST (location);

CREATE INDEX idx_lci_nodes_material_category
    ON lci_nodes (material_category);

CREATE INDEX idx_lci_nodes_type
    ON lci_nodes (type);


-- ============================================================
-- TABLE 8: lci_edges
-- Connections between LCI process nodes
-- Defines the upstream dependency graph
-- Source: FLCAC flow linkages (resolved by flow UUID)
-- ============================================================

CREATE TABLE lci_edges (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_node_id    UUID NOT NULL REFERENCES lci_nodes(id),  -- upstream process
    target_node_id    UUID NOT NULL REFERENCES lci_nodes(id),  -- downstream process
    flow_name         TEXT,                                     -- e.g. "limestone; at quarry"
    flcac_flow_uuid   TEXT,                                     -- FLCAC flow UUID
    amount            DOUBLE PRECISION,
    unit              TEXT,
    material_category TEXT,
    notes             TEXT
);

CREATE INDEX idx_lci_edges_source
    ON lci_edges (source_node_id);

CREATE INDEX idx_lci_edges_target
    ON lci_edges (target_node_id);


-- ============================================================
-- TABLE 9: plant_lci_links
-- Semantic join between EC3 plants and FLCAC process chains
-- Probabilistic — matched by material category + geography
-- Confidence reflects how specific the match is
-- ============================================================

CREATE TABLE plant_lci_links (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plant_id        UUID NOT NULL REFERENCES plants(id),
    lci_node_id     UUID NOT NULL REFERENCES lci_nodes(id),
    confidence      TEXT DEFAULT 'medium',   -- high / medium / low
    match_method    TEXT,                    -- category_match / geo_proximity / manual
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (plant_id, lci_node_id)
);

CREATE INDEX idx_plant_lci_links_plant_id
    ON plant_lci_links (plant_id);

CREATE INDEX idx_plant_lci_links_lci_node_id
    ON plant_lci_links (lci_node_id);


-- ============================================================
-- HELPER FUNCTION: assign_egrid_subregion
-- Given a plant's state, assigns the primary eGRID subregion
-- Used during ETL ingestion
-- Based on EPA eGRID subregion definitions
-- ============================================================

CREATE OR REPLACE FUNCTION assign_egrid_subregion(state_code CHAR(2))
RETURNS TEXT AS $$
BEGIN
    RETURN CASE state_code
        -- New England / NPCC
        WHEN 'ME' THEN 'NPCC'
        WHEN 'NH' THEN 'NPCC'
        WHEN 'VT' THEN 'NPCC'
        WHEN 'MA' THEN 'NPCC'
        WHEN 'RI' THEN 'NPCC'
        WHEN 'CT' THEN 'NPCC'
        -- New York
        WHEN 'NY' THEN 'NPCC'
        -- Mid-Atlantic / RFC
        WHEN 'PA' THEN 'RFC'
        WHEN 'NJ' THEN 'RFC'
        WHEN 'MD' THEN 'RFC'
        WHEN 'DE' THEN 'RFC'
        WHEN 'VA' THEN 'RFC'
        WHEN 'WV' THEN 'RFC'
        WHEN 'OH' THEN 'RFC'
        WHEN 'MI' THEN 'RFC'
        WHEN 'IN' THEN 'RFC'
        WHEN 'IL' THEN 'RFC'
        WHEN 'WI' THEN 'RFC'
        WHEN 'KY' THEN 'RFC'
        -- Southeast / SERC
        WHEN 'NC' THEN 'SERC'
        WHEN 'SC' THEN 'SERC'
        WHEN 'GA' THEN 'SERC'
        WHEN 'FL' THEN 'SERC'
        WHEN 'AL' THEN 'SERC'
        WHEN 'MS' THEN 'SERC'
        WHEN 'TN' THEN 'SERC'
        WHEN 'AR' THEN 'SERC'
        -- Texas / ERCOT
        WHEN 'TX' THEN 'ERCOT'
        -- Midwest / MRO
        WHEN 'MN' THEN 'MRO'
        WHEN 'IA' THEN 'MRO'
        WHEN 'MO' THEN 'MRO'
        WHEN 'ND' THEN 'MRO'
        WHEN 'SD' THEN 'MRO'
        WHEN 'NE' THEN 'MRO'
        WHEN 'KS' THEN 'MRO'
        -- Southwest / WECC
        WHEN 'AZ' THEN 'WECC'
        WHEN 'NM' THEN 'WECC'
        WHEN 'CO' THEN 'WECC'
        WHEN 'UT' THEN 'WECC'
        WHEN 'NV' THEN 'WECC'
        -- Northwest / WECC
        WHEN 'WA' THEN 'WECC'
        WHEN 'OR' THEN 'WECC'
        WHEN 'ID' THEN 'WECC'
        WHEN 'MT' THEN 'WECC'
        WHEN 'WY' THEN 'WECC'
        -- California / WECC
        WHEN 'CA' THEN 'WECC'
        -- SPP
        WHEN 'OK' THEN 'SPP'
        WHEN 'LA' THEN 'SPP'
        -- Default
        ELSE 'UNKNOWN'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- ============================================================
-- TRIGGER: auto-populate location geometry + egrid_subregion
-- Fires on INSERT or UPDATE of lat/lng on plants table
-- ============================================================

CREATE OR REPLACE FUNCTION plants_set_location()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.lat IS NOT NULL AND NEW.lng IS NOT NULL THEN
        NEW.location := ST_SetSRID(ST_MakePoint(NEW.lng, NEW.lat), 4326);
    END IF;
    IF NEW.state IS NOT NULL AND NEW.egrid_subregion IS NULL THEN
        NEW.egrid_subregion := assign_egrid_subregion(NEW.state);
    END IF;
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_plants_set_location
    BEFORE INSERT OR UPDATE ON plants
    FOR EACH ROW
    EXECUTE FUNCTION plants_set_location();


-- ============================================================
-- TRIGGER: auto-populate location geometry on lci_nodes
-- ============================================================

CREATE OR REPLACE FUNCTION lci_nodes_set_location()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.lat IS NOT NULL AND NEW.lng IS NOT NULL THEN
        NEW.location := ST_SetSRID(ST_MakePoint(NEW.lng, NEW.lat), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_lci_nodes_set_location
    BEFORE INSERT OR UPDATE ON lci_nodes
    FOR EACH ROW
    EXECUTE FUNCTION lci_nodes_set_location();


-- ============================================================
-- THE KILLER QUERY (documented here for reference)
-- All concrete plants within 200 miles of Boston
-- whose GWP dropped >15% since 2018
-- with attribution: grid-driven vs. process-driven
-- This is the query that justifies TimescaleDB
-- ============================================================

/*
SELECT
    p.name,
    p.state,
    p.egrid_subregion,
    a.period_start,
    a.period_end,
    a.gwp_start,
    a.gwp_end,
    a.gwp_delta_total,
    a.pct_change_total,
    a.pct_from_grid,
    a.pct_from_process,
    ST_Distance(
        p.location::geography,
        ST_MakePoint(-71.0589, 42.3601)::geography
    ) / 1609.34 AS miles_from_boston
FROM gwp_attribution a
JOIN plants p ON a.plant_id = p.id
WHERE
    p.material_category = 'concrete'
    AND a.period_start >= '2018-01-01'
    AND a.pct_change_total < -15
    AND ST_DWithin(
        p.location::geography,
        ST_MakePoint(-71.0589, 42.3601)::geography,
        321869  -- 200 miles in meters
    )
ORDER BY a.pct_change_total ASC;
*/

-- ============================================================
-- END OF SCHEMA
-- Next steps:
-- 1. Run ec3_ingest.py to populate plants + epd_versions
-- 2. Run egrid_ingest.py to populate grid_carbon
-- 3. Run compute_attribution.py to populate gwp_attribution
-- 4. Run flcac_parser.py to populate lci_nodes + lci_edges
-- 5. Run linker.py to populate plant_lci_links
-- ============================================================
