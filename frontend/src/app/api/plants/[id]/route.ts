import { NextRequest, NextResponse } from 'next/server'
import { queryOne } from '../../db'
export async function GET(_: NextRequest, { params }: { params: { id: string } }) {
  const plant = await queryOne(`
    SELECT p.id::text, p.name, p.manufacturer, p.address, p.city, p.state,
      p.zip, p.lat, p.lng, p.egrid_subregion, p.material_category,
      p.ec3_plant_id, p.created_at,
      COUNT(DISTINCT e.id) AS epd_count,
      MIN(e.issued_at) AS first_epd_date, MAX(e.issued_at) AS latest_epd_date
    FROM plants p LEFT JOIN epd_versions e ON e.plant_id = p.id
    WHERE p.id = $1::uuid GROUP BY p.id
  `, [params.id])
  if (!plant) return NextResponse.json({ detail: 'Not found' }, { status: 404 })
  return NextResponse.json(plant)
}
