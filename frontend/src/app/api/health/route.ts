import { NextResponse } from 'next/server'
import { queryOne } from '../db'

export async function GET() {
  try {
    const plants = await queryOne('SELECT COUNT(*) as plants FROM plants')
    const epds = await queryOne('SELECT COUNT(*) as epds FROM epd_versions')
    return NextResponse.json({ status: 'ok', plants: plants.plants, epds: epds.epds })
  } catch (e: any) {
    return NextResponse.json({ status: 'error', detail: e.message }, { status: 500 })
  }
}
