import { NextRequest, NextResponse } from 'next/server'
import { query } from '../db'
export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams.get('q') || ''
  if (q.length < 2) return NextResponse.json({ results: [], count: 0 })
  const term = `%${q.toLowerCase()}%`
  const exact = `${q.toLowerCase()}%`
  const results = await query(`
    SELECT p.id::text, p.name, p.manufacturer, p.city, p.state,
      p.material_category, p.lat, p.lng, gd.avg_gwp AS latest_gwp
    FROM plants p
    LEFT JOIN LATERAL (SELECT avg_gwp FROM gwp_deltas WHERE plant_id=p.id ORDER BY period DESC LIMIT 1) gd ON TRUE
    WHERE LOWER(p.name) LIKE $1 OR LOWER(p.manufacturer) LIKE $1
    ORDER BY CASE WHEN LOWER(p.name) LIKE $2 THEN 0 ELSE 1 END, p.name LIMIT 20
  `, [term, exact])
  return NextResponse.json({ query: q, results, count: results.length })
}
