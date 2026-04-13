// types.ts
// Shared TypeScript types for the Embodied Carbon Observatory

export interface Plant {
  id: string
  name: string
  manufacturer: string
  city: string
  state: string
  material_category: string
  material_subcategory: string
  lat: number
  lng: number
  egrid_subregion: string
  latest_gwp: number | null
  latest_epd_period: string | null
  gwp_pct_change: number | null
  pct_change_from_grid: number | null
  pct_change_from_process: number | null
  attribution_confidence: string | null
  distance_miles?: number
}

export interface EPDVersion {
  id: string
  ec3_epd_id: string
  issued_at: string
  expired_at: string
  gwp_total: number
  gwp_fossil: number | null
  gwp_biogenic: number | null
  gwp_luluc: number | null
  declared_unit: string
  is_facility_specific: boolean
  is_product_specific: boolean
  program_operator: string
  epd_version: number | null
  grid_co2e_at_issue: number | null
  resource_mix_pct_renewable: number | null
}

export interface Attribution {
  id: string
  period_start: string
  period_end: string
  gwp_start: number
  gwp_end: number
  gwp_delta_total: number
  grid_co2e_start: number
  grid_co2e_end: number
  grid_co2e_delta: number
  gwp_delta_grid: number
  gwp_delta_process: number
  pct_change_total: number
  pct_from_grid: number
  pct_from_process: number
  attribution_confidence: string
  verdict: 'increasing' | 'process_improvement' | 'grid_improvement' | 'mixed'
}

export interface ChainNode {
  data: {
    id: string
    label: string
    type: 'manufacturer' | 'raw_material' | 'processing' | 'transport' | 'energy'
    lat: number | null
    lng: number | null
    region: string | null
    confidence: string | null
    gwp: number | null
    is_root?: boolean
  }
}

export interface ChainEdge {
  data: {
    id: string
    source: string
    target: string
    label: string
    amount: number | null
    unit: string | null
  }
}

export interface GridHistory {
  year: number
  co2e_rate_lb_per_mwh: number
  resource_mix_pct_coal: number | null
  resource_mix_pct_gas: number | null
  resource_mix_pct_nuclear: number | null
  resource_mix_pct_wind: number | null
  resource_mix_pct_solar: number | null
  resource_mix_pct_hydro: number | null
}

export interface Insight {
  plants_with_attribution: number
  plants_improving: number
  plants_worsening: number
  improvement_process_driven: number
  improvement_grid_driven: number
  avg_gwp_change_pct: number
}

export interface MaterialCategory {
  material_category: string
  plant_count: number
  epd_count: number
  avg_gwp: number
}

export type ViewMode = 'map' | 'chart' | 'chain' | 'compare'
