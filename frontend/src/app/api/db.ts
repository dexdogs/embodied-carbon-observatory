import { Pool } from 'pg'

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('sslmode=require') ? { rejectUnauthorized: false } : false,
  max: 10,
})

export async function query(sql: string, params?: any[]) {
  const { rows } = await pool.query(sql, params)
  return rows
}

export async function queryOne(sql: string, params?: any[]) {
  const rows = await query(sql, params)
  return rows[0] || null
}
