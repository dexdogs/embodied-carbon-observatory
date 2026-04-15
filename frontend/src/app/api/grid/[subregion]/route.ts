import { NextRequest, NextResponse } from 'next/server'
import { query } from '../../db'
export async function GET(_: NextRequest, { params: paramsPromise }: { params: Promise<{ subregion: string }> }) {
  const { subregion } = await paramsPromise
  const records = await query(`
    SELECT EXTRACT(year FROM year)::int AS year, co2e_rate_lb_per_mwh,
      resource_mix_pct_wind, resource_mix_pct_solar, resource_mix_pct_hydro
    FROM grid_carbon WHERE egrid_subregion=$1 ORDER BY year ASC
  `, [subregion.toUpperCase()])
  if (!records.length) return NextResponse.json({ detail: 'Not found' }, { status: 404 })
  return NextResponse.json({ subregion: subregion.toUpperCase(), history: records })
}
