import { NextResponse } from 'next/server'
import { query } from '../db'
export async function GET() {
  const results = await query(`
    SELECT p.material_category, COUNT(DISTINCT p.id) AS plant_count,
      COUNT(DISTINCT e.id) AS epd_count
    FROM plants p LEFT JOIN epd_versions e ON e.plant_id=p.id
    GROUP BY p.material_category ORDER BY plant_count DESC
  `)
  return NextResponse.json(results)
}
