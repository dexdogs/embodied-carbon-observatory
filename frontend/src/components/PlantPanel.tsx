'use client'

import { useEffect, useState, useRef } from 'react'
import * as d3 from 'd3'
import { api } from '@/lib/api'
import { Plant, EPDVersion, Attribution } from '@/types'

interface Props {
  plant: Plant
  onClose: () => void
}

type Tab = 'history' | 'attribution' | 'compare'

export default function PlantPanel({ plant, onClose }: Props) {
  const [tab,         setTab]         = useState<Tab>('history')
  const [history,     setHistory]     = useState<any>(null)
  const [attribution, setAttribution] = useState<any>(null)
  const [comparison,  setComparison]  = useState<any>(null)
  const [loading,     setLoading]     = useState(true)
  const chartRef = useRef<SVGSVGElement>(null)

  // Load data when plant changes
  useEffect(() => {
    setLoading(true)
    setHistory(null)
    setAttribution(null)
    setComparison(null)

    Promise.all([
      api.getEPDHistory(plant.id),
      api.getAttribution(plant.id),
      api.getComparison(plant.id),
    ]).then(([h, a, c]) => {
      setHistory(h)
      setAttribution(a)
      setComparison(c)
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [plant.id])

  // Draw D3 chart when history loads
  useEffect(() => {
    if (!history || !chartRef.current || tab !== 'history') return
    drawChart(history)
  }, [history, tab])

  const drawChart = (data: any) => {
    const svg = d3.select(chartRef.current)
    svg.selectAll('*').remove()

    const versions: EPDVersion[] = data.epd_versions || []
    if (versions.length < 1) return

    const W  = 340
    const H  = 180
    const mg = { top: 16, right: 16, bottom: 32, left: 48 }
    const iw = W - mg.left - mg.right
    const ih = H - mg.top - mg.bottom

    svg.attr('width', W).attr('height', H)
       .attr('viewBox', `0 0 ${W} ${H}`)

    const g = svg.append('g').attr('transform', `translate(${mg.left},${mg.top})`)

    const x = d3.scaleTime()
      .domain(d3.extent(versions, d => new Date(d.issued_at)) as [Date, Date])
      .range([0, iw])

    const gwpVals = versions.map(d => d.gwp_total).filter(Boolean) as number[]
    const y = d3.scaleLinear()
      .domain([0, d3.max(gwpVals)! * 1.15])
      .range([ih, 0])

    // Grid lines
    g.append('g')
      .attr('class', 'grid')
      .call(d3.axisLeft(y).ticks(4).tickSize(-iw).tickFormat(() => ''))
      .call(g => {
        g.select('.domain').remove()
        g.selectAll('.tick line')
         .attr('stroke', '#1f3a3a')
         .attr('stroke-dasharray', '2,4')
      })

    // Grid carbon area (background context)
    if (data.epd_versions[0]?.grid_co2e_at_issue) {
      const gridVals = versions
        .filter(d => d.grid_co2e_at_issue)
        .map(d => ({ date: new Date(d.issued_at), val: d.grid_co2e_at_issue as number }))

      if (gridVals.length > 1) {
        const yGrid = d3.scaleLinear()
          .domain([0, d3.max(gridVals, d => d.val)! * 1.2])
          .range([ih, 0])

        const gridLine = d3.line<typeof gridVals[0]>()
          .x(d => x(d.date))
          .y(d => yGrid(d.val))
          .curve(d3.curveMonotoneX)

        g.append('path')
         .datum(gridVals)
         .attr('fill', 'none')
         .attr('stroke', '#f5a623')
         .attr('stroke-width', 1)
         .attr('stroke-dasharray', '3,3')
         .attr('opacity', 0.4)
         .attr('d', gridLine)
      }
    }

    // GWP line
    const line = d3.line<EPDVersion>()
      .x(d => x(new Date(d.issued_at)))
      .y(d => y(d.gwp_total))
      .curve(d3.curveMonotoneX)
      .defined(d => d.gwp_total != null)

    // GWP area fill
    const area = d3.area<EPDVersion>()
      .x(d => x(new Date(d.issued_at)))
      .y0(ih)
      .y1(d => y(d.gwp_total))
      .curve(d3.curveMonotoneX)
      .defined(d => d.gwp_total != null)

    g.append('defs').append('linearGradient')
      .attr('id', 'gwp-gradient')
      .attr('x1', '0').attr('y1', '0')
      .attr('x2', '0').attr('y2', '1')
      .selectAll('stop')
      .data([
        { offset: '0%',   color: '#00e5c8', opacity: 0.25 },
        { offset: '100%', color: '#00e5c8', opacity: 0.02 },
      ])
      .join('stop')
      .attr('offset', d => d.offset)
      .attr('stop-color', d => d.color)
      .attr('stop-opacity', d => d.opacity)

    g.append('path')
     .datum(versions)
     .attr('fill', 'url(#gwp-gradient)')
     .attr('d', area)

    g.append('path')
     .datum(versions)
     .attr('fill', 'none')
     .attr('stroke', '#00e5c8')
     .attr('stroke-width', 2)
     .attr('d', line)

    // Dots
    g.selectAll('.dot')
     .data(versions.filter(d => d.gwp_total != null))
     .join('circle')
     .attr('class', 'dot')
     .attr('cx', d => x(new Date(d.issued_at)))
     .attr('cy', d => y(d.gwp_total))
     .attr('r', 3.5)
     .attr('fill', '#00e5c8')
     .attr('stroke', '#080f0f')
     .attr('stroke-width', 1.5)

    // X axis
    g.append('g')
     .attr('transform', `translate(0,${ih})`)
     .call(d3.axisBottom(x).ticks(4).tickFormat(d3.timeFormat('%Y') as any))
     .call(g => {
       g.select('.domain').attr('stroke', '#1f3a3a')
       g.selectAll('.tick line').remove()
       g.selectAll('.tick text')
        .attr('fill', '#4a7070')
        .attr('font-family', 'IBM Plex Mono')
        .attr('font-size', '10px')
     })

    // Y axis
    g.append('g')
     .call(d3.axisLeft(y).ticks(4))
     .call(g => {
       g.select('.domain').remove()
       g.selectAll('.tick line').remove()
       g.selectAll('.tick text')
        .attr('fill', '#4a7070')
        .attr('font-family', 'IBM Plex Mono')
        .attr('font-size', '10px')
     })

    // Y label
    svg.append('text')
       .attr('transform', 'rotate(-90)')
       .attr('x', -(H / 2))
       .attr('y', 12)
       .attr('text-anchor', 'middle')
       .attr('fill', '#4a7070')
       .attr('font-family', 'IBM Plex Mono')
       .attr('font-size', '9px')
       .text('kg CO₂e / declared unit')
  }

  const trendColor = (pct: number | null) => {
    if (pct === null) return 'var(--muted)'
    if (pct < 0) return 'var(--teal)'
    return 'var(--red)'
  }

  const trendLabel = (pct: number | null) => {
    if (pct === null) return 'No trend data'
    if (pct < -10) return 'Strong improvement'
    if (pct < 0)   return 'Improving'
    if (pct < 5)   return 'Stable'
    return 'Worsening'
  }

  return (
    <div className="flex flex-col h-full overflow-hidden">

      {/* Header */}
      <div className="flex-shrink-0 px-4 pt-4 pb-3"
           style={{ borderBottom: '1px solid var(--border)' }}>
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <h2 className="font-display text-text text-base leading-tight truncate">
              {plant.name}
            </h2>
            {plant.manufacturer && plant.manufacturer !== plant.name && (
              <p className="font-mono text-muted text-xs mt-0.5 truncate">
                {plant.manufacturer}
              </p>
            )}
            <div className="flex items-center gap-2 mt-1.5 flex-wrap">
              <span className="font-mono text-xs px-1.5 py-0.5 rounded-sm"
                    style={{ background: 'var(--surface)', color: 'var(--muted)', fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                {plant.material_category}
              </span>
              <span className="font-mono text-muted text-xs">
                {plant.city && `${plant.city}, `}{plant.state}
              </span>
              {plant.distance_miles && (
                <span className="font-mono text-muted text-xs">
                  {plant.distance_miles} mi
                </span>
              )}
            </div>
          </div>
          <button
            onClick={onClose}
            className="flex-shrink-0 text-muted hover:text-text transition-colors mt-0.5"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M18 6 6 18M6 6l12 12"/>
            </svg>
          </button>
        </div>

        {/* GWP summary */}
        {plant.latest_gwp && (
          <div className="mt-3 flex items-end gap-3">
            <div>
              <div className="font-mono text-xs text-muted uppercase tracking-wider" style={{ fontSize: '9px' }}>
                Latest GWP
              </div>
              <div className="font-mono text-xl font-medium text-text">
                {plant.latest_gwp.toFixed(1)}
                <span className="text-xs text-muted ml-1">kg CO₂e</span>
              </div>
            </div>
            {plant.gwp_pct_change !== null && (
              <div>
                <div className="font-mono text-xs text-muted uppercase tracking-wider" style={{ fontSize: '9px' }}>
                  vs. first EPD
                </div>
                <div className="font-mono text-lg font-medium"
                     style={{ color: trendColor(plant.gwp_pct_change) }}>
                  {plant.gwp_pct_change > 0 ? '+' : ''}{plant.gwp_pct_change?.toFixed(1)}%
                </div>
              </div>
            )}
            <div className="ml-auto text-right">
              <div className="font-mono text-xs"
                   style={{ color: trendColor(plant.gwp_pct_change), fontSize: '10px' }}>
                {trendLabel(plant.gwp_pct_change)}
              </div>
              <div className="font-mono text-muted" style={{ fontSize: '9px' }}>
                {plant.egrid_subregion}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Tabs */}
      <div className="flex-shrink-0 flex"
           style={{ borderBottom: '1px solid var(--border)' }}>
        {(['history', 'attribution', 'compare'] as Tab[]).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className="flex-1 py-2 font-mono text-xs transition-colors"
            style={{
              fontSize:    '10px',
              letterSpacing: '0.06em',
              textTransform: 'uppercase',
              color:       tab === t ? 'var(--teal)' : 'var(--muted)',
              borderBottom: tab === t ? '1px solid var(--teal)' : '1px solid transparent',
              marginBottom: '-1px',
            }}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="flex-1 overflow-y-auto">
        {!loading && !plant.latest_gwp && (
          <div className="p-4 text-center fade-in">
            <div className="font-mono text-xs mb-2" style={{ fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.1em', color: '#f5c542' }}>
              Not yet indexed by Dexdogs
            </div>
            <div className="font-mono text-xs text-muted">
              This plant exists in EC3 but EPD data has not been indexed yet. Check back soon.
            </div>
          </div>
        )}
        {loading && (
          <div className="flex items-center justify-center h-32 text-muted font-mono text-xs pulse">
            Loading...
          </div>
        )}

        {/* HISTORY TAB */}
        {!loading && tab === 'history' && history && (
          <div className="p-4 space-y-4">
            {/* D3 chart */}
            <div>
              <div className="font-mono text-xs text-muted mb-2 uppercase tracking-wider"
                   style={{ fontSize: '9px' }}>
                GWP over time
                <span className="ml-2 text-amber">— grid intensity</span>
              </div>
              <svg ref={chartRef} className="w-full" />
            </div>

            {/* EPD version list */}
            <div>
              <div className="font-mono text-xs text-muted mb-2 uppercase tracking-wider"
                   style={{ fontSize: '9px' }}>
                {history.version_count} EPD versions
              </div>
              <div className="space-y-1">
                {history.epd_versions?.map((v: EPDVersion, i: number) => (
                  <div key={v.id}
                       className="flex items-center justify-between px-2 py-1.5 rounded-sm"
                       style={{ background: 'var(--surface)' }}>
                    <div className="flex items-center gap-2">
                      <div className="w-1.5 h-1.5 rounded-full"
                           style={{ background: 'var(--teal)' }} />
                      <span className="font-mono text-xs text-muted">
                        {new Date(v.issued_at).getFullYear()}
                      </span>
                      {v.is_facility_specific && (
                        <span className="font-mono px-1 rounded"
                              style={{ fontSize: '9px', background: 'rgba(0,229,200,0.1)', color: 'var(--teal)' }}>
                          facility
                        </span>
                      )}
                    </div>
                    <span className="font-mono text-xs text-text">
                      {v.gwp_total?.toFixed(2)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* ATTRIBUTION TAB */}
        {!loading && tab === 'attribution' && attribution && (
          <div className="p-4 space-y-4">
            <div className="font-mono text-xs text-muted uppercase tracking-wider"
                 style={{ fontSize: '9px' }}>
              What drove each GWP change?
            </div>

            {attribution.attributions?.length === 0 && (
              <div className="font-mono text-xs text-muted py-8 text-center">
                Need 2+ EPD versions to compute attribution
              </div>
            )}

            {attribution.attributions?.map((a: Attribution) => (
              <div key={a.id} className="p-3 rounded-sm space-y-3"
                   style={{ background: 'var(--surface)' }}>
                <div className="flex items-center justify-between">
                  <span className="font-mono text-xs text-muted">
                    {new Date(a.period_start).getFullYear()} → {new Date(a.period_end).getFullYear()}
                  </span>
                  <span className="font-mono text-sm font-medium"
                        style={{ color: a.pct_change_total < 0 ? 'var(--teal)' : 'var(--red)' }}>
                    {a.pct_change_total > 0 ? '+' : ''}{a.pct_change_total?.toFixed(1)}%
                  </span>
                </div>

                {/* Attribution bars */}
                <div className="space-y-2">
                  {/* Grid bar */}
                  <div>
                    <div className="flex justify-between mb-1">
                      <span className="font-mono text-muted" style={{ fontSize: '9px' }}>
                        Grid decarbonization
                      </span>
                      <span className="font-mono text-muted" style={{ fontSize: '9px' }}>
                        {Math.abs(a.pct_from_grid).toFixed(0)}%
                      </span>
                    </div>
                    <div className="h-1.5 rounded-full" style={{ background: 'var(--border)' }}>
                      <div className="h-full rounded-full attr-bar"
                           style={{
                             width: `${Math.min(Math.abs(a.pct_from_grid), 100)}%`,
                             background: 'var(--amber)'
                           }} />
                    </div>
                  </div>

                  {/* Process bar */}
                  <div>
                    <div className="flex justify-between mb-1">
                      <span className="font-mono text-muted" style={{ fontSize: '9px' }}>
                        Process improvement
                      </span>
                      <span className="font-mono text-muted" style={{ fontSize: '9px' }}>
                        {Math.abs(a.pct_from_process).toFixed(0)}%
                      </span>
                    </div>
                    <div className="h-1.5 rounded-full" style={{ background: 'var(--border)' }}>
                      <div className="h-full rounded-full attr-bar"
                           style={{
                             width: `${Math.min(Math.abs(a.pct_from_process), 100)}%`,
                             background: 'var(--teal)'
                           }} />
                    </div>
                  </div>
                </div>

                {/* Verdict */}
                <div className="font-mono text-xs"
                     style={{ fontSize: '9px', color: 'var(--muted)' }}>
                  {a.verdict === 'process_improvement' && '→ Plant genuinely improved its process'}
                  {a.verdict === 'grid_improvement'    && '→ Grid got cleaner, not plant efficiency'}
                  {a.verdict === 'mixed'               && '→ Both grid and process contributed'}
                  {a.verdict === 'increasing'          && '⚠ Embodied carbon increased this period'}
                </div>

                <div className="font-mono text-muted" style={{ fontSize: '9px' }}>
                  Confidence: {a.attribution_confidence}
                </div>
              </div>
            ))}

            {/* Summary */}
            {attribution.summary && (
              <div className="p-3 rounded-sm"
                   style={{ background: 'rgba(0,229,200,0.05)', border: '1px solid rgba(0,229,200,0.15)' }}>
                <div className="font-mono text-xs text-muted uppercase tracking-wider mb-2"
                     style={{ fontSize: '9px' }}>
                  Overall
                </div>
                <div className="grid grid-cols-2 gap-2">
                  {[
                    ['Avg change', `${attribution.summary.avg_pct_change}%`],
                    ['Grid driven', `${attribution.summary.avg_pct_from_grid?.toFixed(0)}%`],
                    ['Process driven', `${attribution.summary.avg_pct_from_process?.toFixed(0)}%`],
                    ['Periods', attribution.summary.periods_analyzed],
                  ].map(([label, val]) => (
                    <div key={label as string}>
                      <div className="font-mono text-muted" style={{ fontSize: '9px' }}>{label}</div>
                      <div className="font-mono text-text text-xs">{val}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* COMPARE TAB */}
        {!loading && tab === 'compare' && comparison && (
          <div className="p-4 space-y-3">
            <div className="font-mono text-xs text-muted uppercase tracking-wider"
                 style={{ fontSize: '9px' }}>
              Nearby alternatives within {comparison.radius_miles} miles
            </div>

            {comparison.alternatives?.length === 0 && (
              <div className="font-mono text-xs text-muted py-8 text-center">
                No comparable plants found nearby
              </div>
            )}

            {/* Reference plant */}
            <div className="p-2.5 rounded-sm"
                 style={{ background: 'rgba(0,229,200,0.08)', border: '1px solid rgba(0,229,200,0.2)' }}>
              <div className="flex justify-between items-center">
                <span className="font-mono text-xs text-teal truncate max-w-[70%]">
                  {comparison.reference_plant?.name} (this plant)
                </span>
                <span className="font-mono text-xs text-text">
                  {comparison.reference_plant?.latest_gwp?.toFixed(2) || '—'}
                </span>
              </div>
            </div>

            {/* Alternatives */}
            {comparison.alternatives?.map((alt: any, i: number) => (
              <div key={alt.id}
                   className="p-2.5 rounded-sm flex justify-between items-center"
                   style={{ background: 'var(--surface)' }}>
                <div className="min-w-0">
                  <div className="font-mono text-xs text-text truncate">{alt.name}</div>
                  <div className="font-mono text-muted" style={{ fontSize: '9px' }}>
                    {alt.state} · {alt.distance_miles} mi away
                  </div>
                </div>
                <div className="flex-shrink-0 text-right">
                  <div className="font-mono text-xs text-text">
                    {alt.latest_gwp?.toFixed(2) || '—'}
                  </div>
                  {alt.gwp_trend_pct && (
                    <div className="font-mono" style={{
                      fontSize: '9px',
                      color: alt.gwp_trend_pct < 0 ? 'var(--teal)' : 'var(--red)'
                    }}>
                      {alt.gwp_trend_pct > 0 ? '+' : ''}{alt.gwp_trend_pct?.toFixed(1)}%
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Footer — EC3 attribution */}
      <div className="flex-shrink-0 px-4 py-2"
           style={{ borderTop: '1px solid var(--border)' }}>
        <p className="font-mono text-muted text-center" style={{ fontSize: '9px' }}>
          Data: EC3 / Building Transparency · EPA eGRID · Federal LCA Commons
        </p>
      </div>
    </div>
  )
}
