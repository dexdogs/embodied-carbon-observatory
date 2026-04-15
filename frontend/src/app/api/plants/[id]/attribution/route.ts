import { NextRequest, NextResponse } from 'next/server'
import { query, queryOne } from '../../../db'
export async function GET(_: NextRequest, { params: paramsPromise }: { params: Promise<{ id: string }> }) {
  const { id } = await paramsPromise
  const plant = await queryOne('SELECT id, name, egrid_subregion FROM plants WHERE id = $1::uuid', [id])
  if (!plant) return NextResponse.json({ detail: 'Not found' }, { status: 404 })
  const attributions = await query(`
    SELECT a.id::text, a.period_start, a.period_end,
      a.gwp_start, a.gwp_end, a.gwp_delta_total,
      a.grid_co2e_start, a.grid_co2e_end, a.grid_co2e_delta,
      a.gwp_delta_grid, a.gwp_delta_process,
      a.pct_change_total, a.pct_from_grid, a.pct_from_process,
      a.attribution_confidence,
      CASE WHEN a.pct_change_total >= 0 THEN 'increasing'
           WHEN a.pct_from_process <= -50 THEN 'process_improvement'
           WHEN a.pct_from_grid <= -50 THEN 'grid_improvement'
           ELSE 'mixed' END AS verdict
    FROM gwp_attribution a WHERE a.plant_id = $1::uuid ORDER BY a.period_end ASC
  `, [id])
  const summary = await queryOne(`
    SELECT ROUND(AVG(pct_change_total)::numeric,1)::float AS avg_pct_change,
      ROUND(AVG(pct_from_grid)::numeric,1)::float AS avg_pct_from_grid,
      ROUND(AVG(pct_from_process)::numeric,1)::float AS avg_pct_from_process,
      COUNT(*) AS periods_analyzed
    FROM gwp_attribution WHERE plant_id = $1::uuid
  `, [id])
  return NextResponse.json({ plant_id: id, plant_name: plant.name, subregion: plant.egrid_subregion, attributions, summary })
}
