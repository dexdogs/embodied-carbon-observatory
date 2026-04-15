# Embodied Carbon Observatory

**Geo + Time for EPDs** — a spatial-temporal platform for tracking embodied carbon in US building materials over time.

---

## What It Is

The Embodied Carbon Observatory is a research and analysis tool that maps Environmental Product Declarations (EPDs) for US building material plants and tracks how their embodied carbon intensity changes over time. It answers a question the industry has not had good tools to answer: **when a plant's GWP improves, is that because the plant genuinely changed its process, or because the regional electricity grid got cleaner?**

This distinction matters enormously for procurement, policy, and decarbonization strategy. A concrete plant in New England riding a cleaner grid is telling a very different story than one that changed its cement mix design or sourced lower-carbon aggregates.

---

## Why It Exists

Embodied carbon in building materials accounts for roughly 11% of global greenhouse gas emissions. EPDs are the primary disclosure mechanism — but they are typically evaluated as point-in-time snapshots. No tool existed to:

- Track a plant's GWP trajectory across multiple EPD vintages
- Decompose that trajectory into grid-driven vs. process-driven components
- Visualize the geographic distribution of improvement (and deterioration) across the US supply base
- Compare a specifier's local options by both current GWP and improvement trajectory

The Observatory is built to fill that gap, with a focus on ready-mix concrete as the initial material category given its data density in public EPD registries.

---

## How It Works

### Attribution Model

For each plant with two or more EPD vintages, the Observatory computes a pairwise attribution between consecutive years:

**Grid contribution** = GWP_start × (grid_delta / grid_start)

Where `grid_delta` is the change in the regional grid's CO₂e emission rate (lb/MWh) between the two EPD issue years, using EPA eGRID subregion data matched to the plant's location via PostGIS spatial join.

**Process contribution** = Total GWP delta − Grid contribution

This decomposition reveals whether a plant's trajectory is driven by external grid decarbonization or by genuine operational change.

### Dot Language on the Map

- **Bright yellow pulsing dot** — plant has 2+ years of EPD data; temporal attribution is available
- **Green dot** — plant has at least one EPD indexed
- **Gray dot** — plant exists in EC3 but has not yet been indexed

---

## Datasets

| Dataset | Source | Coverage | Use |
|---------|--------|----------|-----|
| Environmental Product Declarations | EC3 / Building Transparency (openEPD API) | ~14,000 EPDs, US and international | GWP values, declared units, issue dates, plant linkage |
| Plant registry | EC3 / Building Transparency | ~5,600 plants | Facility names, coordinates, manufacturer, material category |
| eGRID Subregion Emission Rates | US EPA eGRID 2018–2023 | 27 NERC subregions, annual | Grid CO₂e rate (lb/MWh), renewable mix by subregion and year |
| eGRID Subregion Shapefiles | US EPA | 2022 boundary polygons | Spatial assignment of plants to grid subregions via PostGIS |
| Federal LCA Commons (FLCAC) | US EPA / NREL | US background LCI processes | Supply chain node data (ingestion pipeline built, not yet fully connected) |

---

## Tech Stack

### Data Pipeline
- **Python 3.11** — ingestion, transformation, attribution computation
- **psycopg2** — PostgreSQL/TimescaleDB client
- **pandas** — EPD parsing and eGRID Excel processing
- **openpyxl** — eGRID xlsx ingestion
- **requests** — EC3 API pagination
- **tqdm** — progress tracking

### Database
- **TimescaleDB** (PostgreSQL extension) — time-series optimized storage for EPD versions and grid data
- **PostGIS** — spatial indexing and plant-to-subregion assignment via ST_Within and ST_DWithin
- Key tables: `plants`, `epd_versions`, `grid_carbon`, `gwp_attribution`, `gwp_deltas`, `egrid_subregions`

### Infrastructure
- **Vercel** — frontend and API deployment
- **TimescaleDB Cloud** — managed database
- **GitHub Codespaces** — development environment

---

## Data Attribution

EPD data sourced from the [EC3 platform](https://buildingtransparency.org) by Building Transparency under their public API terms. Grid emission data from the [EPA eGRID program](https://www.epa.gov/egrid). Supply chain background data from the [Federal LCA Commons](https://www.lcacommons.gov).
