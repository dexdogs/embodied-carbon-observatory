'use client'

import { useEffect, useState } from 'react'
import { api } from '@/lib/api'

// ── InsightsBanner ───────────────────────────────────────────
// Shows headline findings bottom-left of the map

interface InsightsProps {
  category: string | null
}

export function InsightsBanner({ category }: InsightsProps) {
  const [data, setData] = useState<any>(null)

  useEffect(() => {
    api.getInsights({ category: category || undefined })
      .then(setData)
      .catch(() => {})
  }, [category])

  if (!data?.summary) return null

  const s = data.summary
  const improvingPct = s.plants_with_attribution > 0
    ? Math.round((s.plants_improving / s.plants_with_attribution) * 100)
    : 0

  return (
    <div className="panel p-3 fade-in"
         style={{ background: 'rgba(8,15,15,0.92)' }}>
      <div className="font-mono text-muted uppercase tracking-wider mb-2"
           style={{ fontSize: '9px' }}>
        {category || 'All materials'} · US overview
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-1.5">
        <div>
          <div className="font-mono text-teal text-lg font-medium leading-none">
            {improvingPct}%
          </div>
          <div className="font-mono text-muted" style={{ fontSize: '9px' }}>
            of plants improving
          </div>
        </div>

        <div>
          <div className="font-mono text-amber text-lg font-medium leading-none">
            {s.improvement_grid_driven > 0 && s.plants_improving > 0
              ? Math.round((s.improvement_grid_driven / s.plants_improving) * 100)
              : 0}%
          </div>
          <div className="font-mono text-muted" style={{ fontSize: '9px' }}>
            grid-driven
          </div>
        </div>

        <div>
          <div className="font-mono text-text text-sm font-medium leading-none">
            {s.avg_gwp_change_pct?.toFixed(1)}%
          </div>
          <div className="font-mono text-muted" style={{ fontSize: '9px' }}>
            avg GWP change
          </div>
        </div>

        <div>
          <div className="font-mono text-teal text-sm font-medium leading-none">
            {s.improvement_process_driven > 0 && s.plants_improving > 0
              ? Math.round((s.improvement_process_driven / s.plants_improving) * 100)
              : 0}%
          </div>
          <div className="font-mono text-muted" style={{ fontSize: '9px' }}>
            process-driven
          </div>
        </div>
      </div>
    </div>
  )
}


// ── Legend ───────────────────────────────────────────────────
// Color scale explanation bottom-right of the map

export function Legend() {
  const items = [
    { color: '#f5c542', label: 'Indexed for demo' },
    { color: '#4a9e6b', label: 'EPD only — no temporal graph' },
    { color: '#6b6b6b', label: 'Not part of demo — access upon request' },
  ]

  return (
    <div className="panel p-3 fade-in"
         style={{ background: 'rgba(8,15,15,0.92)' }}>
      <div className="font-mono text-muted uppercase tracking-wider mb-2"
           style={{ fontSize: '9px' }}>
        GWP trend
      </div>
      <div className="space-y-1">
        {items.map(item => (
          <div key={item.label} className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full flex-shrink-0"
                 style={{ background: item.color }} />
            <span className="font-mono text-muted" style={{ fontSize: '9px' }}>
              {item.label}
            </span>
          </div>
        ))}
      </div>

    </div>
  )
}

export default InsightsBanner
