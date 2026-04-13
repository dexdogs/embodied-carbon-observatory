'use client'

import { useState, useEffect } from 'react'
import { api } from '@/lib/api'

const US_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
  'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
  'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
  'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
  'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
]

interface Props {
  selectedCategory: string | null
  selectedState:    string | null
  onCategoryChange: (cat: string | null) => void
  onStateChange:    (state: string | null) => void
}

export default function FilterBar({ selectedCategory, selectedState, onCategoryChange, onStateChange }: Props) {
  const [categories, setCategories] = useState<any[]>([])

  useEffect(() => {
    api.getMaterials().then(setCategories).catch(() => {})
  }, [])

  const chip = (label: string, active: boolean, onClick: () => void) => (
    <button
      key={label}
      onClick={onClick}
      className="font-mono text-xs px-2.5 py-1 rounded-sm transition-all"
      style={{
        background:   active ? 'var(--teal)' : 'rgba(26,46,46,0.9)',
        border:       `1px solid ${active ? 'var(--teal)' : 'var(--border)'}`,
        color:        active ? '#080f0f' : 'var(--muted)',
        fontSize:     '10px',
        letterSpacing: '0.06em',
        textTransform: 'uppercase',
      }}
    >
      {label}
    </button>
  )

  return (
    <div className="flex items-center gap-1.5 flex-wrap">
      {/* Category chips */}
      {chip('All', !selectedCategory, () => onCategoryChange(null))}
      {categories.slice(0, 8).map((c: any) =>
        chip(
          c.material_category,
          selectedCategory === c.material_category,
          () => onCategoryChange(
            selectedCategory === c.material_category ? null : c.material_category
          )
        )
      )}

      {/* State dropdown */}
      <select
        value={selectedState || ''}
        onChange={e => onStateChange(e.target.value || null)}
        className="font-mono text-xs px-2 py-1 outline-none rounded-sm"
        style={{
          background:  'rgba(26,46,46,0.9)',
          border:      `1px solid ${selectedState ? 'var(--teal)' : 'var(--border)'}`,
          color:       selectedState ? 'var(--teal)' : 'var(--muted)',
          fontSize:    '10px',
          letterSpacing: '0.06em',
        }}
      >
        <option value="">All states</option>
        {US_STATES.map(s => (
          <option key={s} value={s}>{s}</option>
        ))}
      </select>
    </div>
  )
}