// lib/api.ts
// All API calls to the FastAPI backend

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

async function apiFetch<T>(path: string, params?: Record<string, string | number | boolean>): Promise<T> {
  const url = new URL(`${API_URL}${path}`)
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) {
        url.searchParams.set(k, String(v))
      }
    })
  }
  const res = await fetch(url.toString())
  if (!res.ok) throw new Error(`API error ${res.status}: ${path}`)
  return res.json()
}

export const api = {
  // Plants
  getPlants: (params: {
    lat?: number
    lng?: number
    radius_miles?: number
    category?: string
    state?: string
    search?: string
    limit?: number
  }) => apiFetch<any>('/plants', params as any),

  getPlant: (id: string) =>
    apiFetch<any>(`/plants/${id}`),

  getEPDHistory: (id: string) =>
    apiFetch<any>(`/plants/${id}/epd-history`),

  getAttribution: (id: string) =>
    apiFetch<any>(`/plants/${id}/attribution`),

  getChain: (id: string) =>
    apiFetch<any>(`/plants/${id}/chain`),

  getComparison: (id: string, radius_miles?: number) =>
    apiFetch<any>(`/plants/${id}/compare`, radius_miles ? { radius_miles } : undefined),

  // Search
  search: (q: string) =>
    apiFetch<any>('/search', { q }),

  // Materials
  getMaterials: () =>
    apiFetch<any>('/materials'),

  // Insights
  getInsights: (params?: { category?: string; state?: string }) =>
    apiFetch<any>('/insights', params as any),

  // Grid
  getGridHistory: (subregion: string) =>
    apiFetch<any>(`/grid/${subregion}`),
}
