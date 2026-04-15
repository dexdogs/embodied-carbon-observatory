'use client'

import { useState, useRef, useEffect } from 'react'
import { api } from '@/lib/api'
import { Plant } from '@/types'

interface Props {
  onResult: (plant: Plant) => void
}

export default function SearchBar({ onResult }: Props) {
  const [query,   setQuery]   = useState('')
  const [results, setResults] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [open,    setOpen]    = useState(false)
  const debounce = useRef<NodeJS.Timeout>()
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (query.length < 2) {
      setResults([])
      setOpen(false)
      return
    }

    clearTimeout(debounce.current)
    debounce.current = setTimeout(async () => {
      setLoading(true)
      try {
        const data = await api.search(query)
        setResults(data.results || [])
        setOpen(true)
      } catch {
        setResults([])
      } finally {
        setLoading(false)
      }
    }, 300)
  }, [query])

  const handleSelect = (plant: any) => {
    setQuery(plant.name)
    setOpen(false)
    onResult(plant)
  }

  const handleKeyDown = (e: any) => {
    if (e.key === 'Enter' && results.length > 0) {
      handleSelect(results[0])
    }
  }

  const gwpColor = (pct: number | null) => {
    if (pct === null) return '#4a7070'
    if (pct < 0) return '#00e5c8'
    return '#ff4444'
  }

  return (
    <div className="relative">
      {/* Input */}
      <div className="relative"
           style={{ background: 'rgba(26,46,46,0.95)', border: '1px solid var(--border)', borderRadius: 4 }}>
        <div className="absolute left-3 top-1/2 -translate-y-1/2 text-muted">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
          </svg>
        </div>
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Search plants or manufacturers..."
          className="w-full bg-transparent pl-8 pr-3 py-2.5 text-text placeholder-muted outline-none font-mono text-xs"
          style={{ fontSize: '12px' }}
        />
        {loading && (
          <div className="absolute right-3 top-1/2 -translate-y-1/2 text-muted pulse">
            <div className="w-2 h-2 rounded-full bg-teal" />
          </div>
        )}
      </div>

      {/* Dropdown */}
      {open && results.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-1 fade-in"
             style={{ background: 'rgba(13,26,26,0.98)', border: '1px solid var(--border)', borderRadius: 4, maxHeight: 320, overflowY: 'auto', zIndex: 50 }}>
          {results.map((r: any) => (
            <button
              key={r.id}
              onClick={() => handleSelect(r)}
              className="w-full text-left px-3 py-2.5 hover:bg-panel transition-colors border-b border-border last:border-0"
            >
              <div className="flex items-center justify-between gap-2">
                <div className="min-w-0">
                  <div className="font-mono text-xs text-text truncate">{r.name}</div>
                  <div className="font-mono text-xs text-muted truncate">
                    {r.manufacturer && <span>{r.manufacturer} · </span>}
                    {r.city && <span>{r.city}, </span>}
                    <span>{r.state}</span>
                  </div>
                </div>
                <div className="flex-shrink-0 text-right">
                  <div className="font-mono text-xs"
                       style={{ color: '#4a7070', textTransform: 'uppercase', fontSize: '10px' }}>
                    {r.material_category}
                  </div>
                  {r.latest_gwp && (
                    <div className="font-mono text-xs"
                         style={{ color: gwpColor(r.gwp_pct_change) }}>
                      {r.latest_gwp.toFixed(1)}
                    </div>
                  )}
                </div>
              </div>
            </button>
          ))}
        </div>
      )}

      {open && results.length === 0 && !loading && query.length >= 2 && (
        <div className="absolute top-full left-0 right-0 mt-1 px-3 py-2.5 font-mono text-xs text-muted"
             style={{ background: 'rgba(13,26,26,0.98)', border: '1px solid var(--border)', borderRadius: 4 }}>
          No plants found for "{query}"
        </div>
      )}
    </div>
  )
}
