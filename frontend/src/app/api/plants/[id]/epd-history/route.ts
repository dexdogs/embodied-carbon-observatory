import { NextRequest, NextResponse } from 'next/server'
import { query, queryOne } from '../../../db'
export async function GET(_: NextRequest, { params: paramsPromise }: { params: Promise<{ id: string }> }) {
  const { id } = await paramsPromise
  const plant = await queryOne('SELECT id, name, material_category, egrid_subregion FROM plants WHERE id = $1::uuid', [id])
  if (!plant) return NextResponse.json({ detail: 'Not found' }, { status: 404 })
  const versions = await query(`
    SELECT e.id::text, e.ec3_epd_id, e.issued_at, e.gwp_total, e.gwp_fossil,
      e.gwp_biogenic, e.declared_unit, e.is_facility_specific, e.is_product_specific,
      g.co2e_rate_lb_per_mwh AS grid_co2e_at_issue,
      (COALESCE(g.resource_mix_pct_wind,0)+COALESCE(g.resource_mix_pct_solar,0)+COALESCE(g.resource_mix_pct_hydro,0)) AS resource_mix_pct_renewable
    FROM epd_versions e
    LEFT JOIN plants p ON p.id = e.plant_id
    LEFT JOIN grid_carbon g ON g.egrid_subregion = p.egrid_subregion
      AND EXTRACT(year FROM g.year) = EXTRACT(year FROM e.issued_at)
    WHERE e.plant_id = $1::uuid AND e.gwp_total IS NOT NULL
    ORDER BY e.issued_at ASC
  `, [id])
  return NextResponse.json({ plant_id: id, plant_name: plant.name, material_category: plant.material_category, epd_versions: versions, version_count: versions.length })
}
