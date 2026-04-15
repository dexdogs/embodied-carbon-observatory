import { NextRequest, NextResponse } from 'next/server'
import { queryOne } from '../db'
export async function GET(req: NextRequest) {
  const category = req.nextUrl.searchParams.get('category')
  const state = req.nextUrl.searchParams.get('state')
  const conditions: string[] = []
  const params: any[] = []
  if (category) { params.push(category.toLowerCase()); conditions.push(`p.material_category=$${params.length}`) }
  if (state) { params.push(state.toUpperCase()); conditions.push(`p.state=$${params.length}`) }
  const where = conditions.length ? 'AND ' + conditions.join(' AND ') : ''
  const summary = await queryOne(`
    SELECT COUNT(DISTINCT a.plant_id) AS plants_with_attribution,
      COUNT(DISTINCT a.plant_id) FILTER (WHERE a.pct_change_total < 0) AS plants_improving,
      COUNT(DISTINCT a.plant_id) FILTER (WHERE a.pct_change_total > 0) AS plants_worsening,
      ROUND(AVG(a.pct_change_total)::numeric,1) AS avg_gwp_change_pct
    FROM gwp_attribution a JOIN plants p ON a.plant_id=p.id
    WHERE a.pct_change_total IS NOT NULL ${where}
  `, params)
  return NextResponse.json({ summary, filters: { category, state } })
}
