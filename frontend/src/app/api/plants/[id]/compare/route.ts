import { NextRequest, NextResponse } from 'next/server'
import { query, queryOne } from '../../../db'
export async function GET(req: NextRequest, { params }: { params: { id: string } }) {
  const radius_miles = parseFloat(req.nextUrl.searchParams.get('radius_miles') || '200')
  const plant = await queryOne('SELECT id, name, material_category, lat, lng FROM plants WHERE id = $1::uuid', [params.id])
  if (!plant) return NextResponse.json({ detail: 'Not found' }, { status: 404 })
  const radius_meters = radius_miles * 1609.34
  const alternatives = await query(`
    SELECT p.id::text, p.name, p.state, p.lat, p.lng,
      ROUND((ST_Distance(p.location::geography,ST_MakePoint($1,$2)::geography)/1609.34)::numeric,1) AS distance_miles,
      gd.avg_gwp AS latest_gwp, a.pct_change_total AS gwp_trend_pct
    FROM plants p
    LEFT JOIN LATERAL (SELECT avg_gwp FROM gwp_deltas WHERE plant_id=p.id ORDER BY period DESC LIMIT 1) gd ON TRUE
    LEFT JOIN LATERAL (SELECT pct_change_total FROM gwp_attribution WHERE plant_id=p.id ORDER BY period_end DESC LIMIT 1) a ON TRUE
    WHERE p.material_category=$3 AND p.id!=$4::uuid
      AND ST_DWithin(p.location::geography,ST_MakePoint($1,$2)::geography,$5)
      AND gd.avg_gwp IS NOT NULL
    ORDER BY gd.avg_gwp ASC LIMIT 5
  `, [plant.lng, plant.lat, plant.material_category, params.id, radius_meters])
  const ref = await queryOne('SELECT avg_gwp FROM gwp_deltas WHERE plant_id=$1::uuid ORDER BY period DESC LIMIT 1', [params.id])
  return NextResponse.json({ reference_plant: { id: params.id, name: plant.name, latest_gwp: ref?.avg_gwp }, alternatives, radius_miles })
}
