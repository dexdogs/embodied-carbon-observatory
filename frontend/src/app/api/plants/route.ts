import { NextRequest, NextResponse } from 'next/server'
import { query } from '../db'

export async function GET(req: NextRequest) {
  const s = req.nextUrl.searchParams
  const category = s.get('category')
  const state = s.get('state')
  const search = s.get('search')
  const limit = Math.min(parseInt(s.get('limit') || '500'), 2000)
  const lat = s.get('lat') ? parseFloat(s.get('lat')!) : null
  const lng = s.get('lng') ? parseFloat(s.get('lng')!) : null

  const conditions = ['p.lat IS NOT NULL', 'p.lng IS NOT NULL', "p.state IS NOT NULL", "p.state != ''", "p.egrid_subregion != 'UNKNOWN'"]
  const params: any[] = []

  if (category) { params.push(category.toLowerCase()); conditions.push(`p.material_category = $${params.length}`) }
  if (state) { params.push(state.toUpperCase()); conditions.push(`p.state = $${params.length}`) }
  if (search) { params.push(`%${search.toLowerCase()}%`); conditions.push(`(LOWER(p.name) LIKE $${params.length} OR LOWER(p.manufacturer) LIKE $${params.length})`) }

  const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : ''
  params.push(limit)

  const sql = `
    SELECT p.id::text, p.name, p.manufacturer, p.city, p.state,
      p.material_category, p.material_subcategory, p.lat, p.lng, p.egrid_subregion,
      gd.avg_gwp AS latest_gwp, gd.period AS latest_epd_period,
      a.pct_change_total AS gwp_pct_change,
      a.pct_from_grid AS pct_change_from_grid,
      a.pct_from_process AS pct_change_from_process,
      a.attribution_confidence,
      CASE WHEN epd_years.year_count >= 2 THEN true ELSE false END AS has_temporal
    FROM plants p
    LEFT JOIN LATERAL (
      SELECT avg_gwp, period FROM gwp_deltas WHERE plant_id = p.id ORDER BY period DESC LIMIT 1
    ) gd ON TRUE
    LEFT JOIN LATERAL (
      SELECT pct_change_total, pct_from_grid, pct_from_process, attribution_confidence
      FROM gwp_attribution WHERE plant_id = p.id ORDER BY period_end DESC LIMIT 1
    ) a ON TRUE
    LEFT JOIN LATERAL (
      SELECT COUNT(DISTINCT EXTRACT(year FROM issued_at)) AS year_count
      FROM epd_versions WHERE plant_id = p.id AND gwp_total IS NOT NULL
    ) epd_years ON TRUE
    ${where}
    ORDER BY gd.avg_gwp IS NULL ASC, p.name
    LIMIT $${params.length}
  `

  const rows = await query(sql, params)
  const features = rows.map(r => ({
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [r.lng, r.lat] },
    properties: Object.fromEntries(Object.entries(r).filter(([k]) => k !== 'lat' && k !== 'lng'))
  }))
  return NextResponse.json({ type: 'FeatureCollection', features, count: features.length })
}
