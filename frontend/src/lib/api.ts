const API_URL = typeof window !== 'undefined' ? '' : (process.env.NEXT_PUBLIC_API_URL || '')

async function apiFetch<T>(path: string, params?: Record<string, string | number | boolean>): Promise<T> {
  const url = new URL(`${API_URL}${path}`, typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3000')
  if (params) Object.entries(params).forEach(([k, v]) => { if (v !== undefined && v !== null) url.searchParams.set(k, String(v)) })
  const res = await fetch(url.toString())
  if (!res.ok) throw new Error(`API error ${res.status}: ${path}`)
  return res.json()
}

export const api = {
  getPlants: (params: any) => apiFetch<any>('/api/plants', params),
  getPlant: (id: string) => apiFetch<any>(`/api/plants/${id}`),
  getEPDHistory: (id: string) => apiFetch<any>(`/api/plants/${id}/epd-history`),
  getAttribution: (id: string) => apiFetch<any>(`/api/plants/${id}/attribution`),
  getChain: (id: string) => apiFetch<any>(`/api/plants/${id}/chain`),
  getComparison: (id: string, radius_miles?: number) => apiFetch<any>(`/api/plants/${id}/compare`, radius_miles ? { radius_miles } : undefined),
  search: (q: string) => apiFetch<any>('/api/search', { q }),
  getMaterials: () => apiFetch<any>('/api/materials'),
  getInsights: (params?: any) => apiFetch<any>('/api/insights', params),
  getGridHistory: (subregion: string) => apiFetch<any>(`/api/grid/${subregion}`),
}
