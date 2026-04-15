'use client'

import { useEffect, useRef, useState, useCallback } from 'react'
import { api } from '@/lib/api'
import { Plant } from '@/types'
import SearchBar from '@/components/SearchBar'
import FilterBar from '@/components/FilterBar'
import PlantPanel from '@/components/PlantPanel'
import InfoFeedback from '@/components/InfoFeedback'
import MapSummary from '@/components/MapSummary'
import { InsightsBanner, Legend } from '@/components/InsightsBanner'

const MAPBOX_TOKEN = process.env.NEXT_PUBLIC_MAPBOX_TOKEN || ''

function gwpColor(hasTemporal: boolean, hasEPD: boolean): string {
  if (hasTemporal) return '#FFFF00'   // yellow — multi-year temporal data
  if (hasEPD)      return '#4a9e6b'   // light green — has EPDs but single year
  return '#6b6b6b'                     // gray — not yet indexed
}
export default function Home() {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map          = useRef<any>(null)
  const [selectedPlant, setSelectedPlant]       = useState<Plant | null>(null)
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null)
  const [selectedState, setSelectedState]       = useState<string | null>(null)
  const [plantCount, setPlantCount]             = useState(0)
  const [indexedCount, setIndexedCount]         = useState(0)
  const [requestCount, setRequestCount]         = useState(0)
  const [singleEpdCount, setSingleEpdCount]     = useState(0)
  const [mapReady, setMapReady]                 = useState(false)
  const mapboxglRef = useRef<any>(null)
  const popupRef = useRef<any>(null)

  const closePlantPopup = useCallback(() => {
    if (popupRef.current) {
      popupRef.current.remove()
      popupRef.current = null
    }
  }, [])

  const showPlantPopup = useCallback(async (plant: any, coords: [number, number]) => {
    if (!map.current || !mapboxglRef.current) return

    try {
      const [details, attribution] = await Promise.all([
        api.getPlant(plant.id),
        api.getAttribution(plant.id),
      ])

      const overall = attribution.summary?.avg_pct_change ?? attribution.attributions?.[0]?.pct_change_total ?? null
      const grid = attribution.summary?.avg_pct_from_grid ?? attribution.attributions?.[0]?.pct_from_grid ?? null
      const process = attribution.summary?.avg_pct_from_process ?? attribution.attributions?.[0]?.pct_from_process ?? null
      const periods = attribution.attributions?.length ?? 0
      const address = details.address || `${details.city || ''}${details.city && details.state ? ', ' : ''}${details.state || ''}`

      const html = `
        <div style="font-family: IBM Plex Mono, monospace; color: #f5f5f5; font-size: 12px; line-height: 1.4; padding: 10px; max-width: 240px;">
          <div style="font-weight: 700; margin-bottom: 6px;">${plant.name}</div>
          <div style="color: #8a98a4; font-size: 11px; margin-bottom: 10px;">${address}</div>
          <div style="font-size: 11px; color: #8a98a4; text-transform: uppercase; margin-bottom: 6px; letter-spacing: 0.08em;">Overall</div>
          <div style="font-weight: 700; font-size: 14px; margin-bottom: 8px;">${overall !== null ? `${overall > 0 ? '+' : ''}${overall.toFixed(1)}%` : '—'}</div>
          <div style="display: grid; grid-template-columns: 1fr auto; gap: 4px 8px; font-size: 11px;">
            <span style="color: #8a98a4;">Grid driven</span><span style="font-weight: 700;">${grid !== null ? `${grid > 0 ? '+' : ''}${grid.toFixed(1)}%` : '—'}</span>
            <span style="color: #8a98a4;">Process driven</span><span style="font-weight: 700;">${process !== null ? `${process > 0 ? '+' : ''}${process.toFixed(1)}%` : '—'}</span>
            <span style="color: #8a98a4;">Periods</span><span style="font-weight: 700;">${periods}</span>
          </div>
        </div>
      `

      closePlantPopup()
      popupRef.current = new mapboxglRef.current.Popup({ closeButton: false, closeOnClick: false, offset: 15 })
        .setLngLat(coords)
        .setHTML(html)
        .addTo(map.current)
    } catch (error) {
      console.error('Failed to load plant popup data:', error)
    }
  }, [closePlantPopup])

  const handlePlantSelect = useCallback(async (plant: any, coords: [number, number]) => {
    setSelectedPlant(plant)
    if (map.current) {
      map.current.setFilter('plants-selected', ['==', ['get', 'id'], plant.id])
    }
    await showPlantPopup(plant, coords)
  }, [showPlantPopup])

  // Init map
  useEffect(() => {
    if (map.current || !mapContainer.current) return

    const initMap = async () => {
      const mapboxgl = (await import('mapbox-gl')).default
      await import('mapbox-gl/dist/mapbox-gl.css')
      mapboxgl.accessToken = MAPBOX_TOKEN
      mapboxglRef.current = mapboxgl

      const m = new mapboxgl.Map({
        container: mapContainer.current!,
        style: 'mapbox://styles/mapbox/dark-v11',
        center: [-95, 38],
        zoom: 4,
      })

      map.current = m

      m.on('load', () => {
        m.addSource('plants', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
        m.addSource('chain-edges', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
        m.addSource('chain-nodes', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })

        m.addLayer({ id: 'chain-edges-layer', type: 'line', source: 'chain-edges',
          paint: { 'line-color': '#00e5c8', 'line-width': 1.5, 'line-opacity': 0.6, 'line-dasharray': [2, 2] } })

        m.addLayer({ id: 'plants-halo', type: 'circle', source: 'plants',
          paint: { 'circle-radius': 10, 'circle-color': ['get', 'dot_color'], 'circle-opacity': 0.15, 'circle-blur': 0.8 } })

        // Pulsing ring for temporal plants — animated via requestAnimationFrame
        m.addLayer({ id: 'plants-pulse', type: 'circle', source: 'plants',
          filter: ['==', ['get', 'has_temporal'], true],
          paint: {
            'circle-radius': 10,
            'circle-color': 'transparent',
            'circle-stroke-width': 2,
            'circle-stroke-color': '#FFE000',
            'circle-stroke-opacity': 0.9,
          }
        })

        // Animate pulse radius
        let pulseSize = 10
        let growing = true
        setInterval(() => {
          if (growing) { pulseSize += 0.4; if (pulseSize >= 18) growing = false }
          else         { pulseSize -= 0.4; if (pulseSize <= 10) growing = true  }
          if (m.getLayer('plants-pulse')) {
            m.setPaintProperty('plants-pulse', 'circle-radius', pulseSize)
            m.setPaintProperty('plants-pulse', 'circle-stroke-opacity', 
              0.9 * (1 - (pulseSize - 10) / 8))
          }
        }, 30)

        m.addLayer({ id: 'plants-layer', type: 'circle', source: 'plants',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 4, ['get', 'dot_size'], 10, ['*', ['get', 'dot_size'], 1.8]],
            'circle-color': ['get', 'dot_color'],
            'circle-opacity': 1.0,
            'circle-stroke-width': 1.5,
            'circle-stroke-color': 'rgba(255,255,255,0.5)',
          }
        })

        m.addLayer({ id: 'plants-selected', type: 'circle', source: 'plants',
          filter: ['==', ['get', 'id'], ''],
          paint: { 'circle-radius': 14, 'circle-color': 'transparent', 'circle-stroke-width': 2, 'circle-stroke-color': '#00e5c8' }
        })

        m.on('mouseenter', 'plants-layer', () => { m.getCanvas().style.cursor = 'pointer' })
        m.on('mouseleave', 'plants-layer', () => { m.getCanvas().style.cursor = '' })

        m.on('click', 'plants-layer', (e: any) => {
          if (!e.features?.[0]) return
          const p = e.features[0].properties as Plant
          const coords = (e.features[0].geometry as any).coordinates
          handlePlantSelect(p, coords)
          m.flyTo({ center: coords, zoom: Math.max(m.getZoom(), 8), duration: 800 })
        })

        m.on('click', (e: any) => {
          const f = m.queryRenderedFeatures(e.point, { layers: ['plants-layer'] })
          if (f.length === 0) {
            setSelectedPlant(null)
            m.setFilter('plants-selected', ['==', ['get', 'id'], ''])
            closePlantPopup()
          }
        })

        setMapReady(true)
      })

      // Get user location
      navigator.geolocation?.getCurrentPosition((pos) => {
        m.flyTo({ center: [pos.coords.longitude, pos.coords.latitude], zoom: 7, duration: 1500 })
      })
    }

    initMap()
    return () => { map.current?.remove(); map.current = null }
  }, [])

  // Load plants
  useEffect(() => {
    if (!mapReady) return
    const loadPlants = async () => {
      try {
        const params: any = { limit: 2000 }
        if (selectedCategory) params.category = selectedCategory
        if (selectedState) params.state = selectedState
        const geojson = await api.getPlants(params)
        const enriched = {
          ...geojson,
          features: geojson.features.map((f: any) => ({
            ...f,
            properties: {
              ...f.properties,
              dot_color: gwpColor(f.properties.has_temporal === true, f.properties.latest_gwp !== null),
              dot_size: f.properties.has_temporal === true ? 8 : f.properties.latest_gwp !== null ? 6 : 4,
              has_temporal: f.properties.has_temporal,
            }
          }))
        }

        const total = geojson.count
        const indexed = enriched.features.filter((f: any) => f.properties.has_temporal === true).length
        const oneEpd = enriched.features.filter((f: any) => f.properties.latest_gwp !== null && f.properties.has_temporal !== true).length
        const notIndexed = enriched.features.filter((f: any) => f.properties.latest_gwp === null).length

        const source = map.current?.getSource('plants') as any
        source?.setData(enriched)
        setPlantCount(total)
        setIndexedCount(indexed)
        setSingleEpdCount(oneEpd)
        setRequestCount(notIndexed)
      } catch (err) {
        console.error('Failed to load plants:', err)
      }
    }
    loadPlants()
  }, [mapReady, selectedCategory, selectedState])

  const handleSearchResult = useCallback((plant: Plant) => {
    map.current?.flyTo({ center: [plant.lng, plant.lat], zoom: 10, duration: 1200 })
    handlePlantSelect(plant, [plant.lng, plant.lat])
  }, [handlePlantSelect])

  return (
    <main style={{ position: "relative", width: "100vw", height: "100vh", overflow: "hidden", background: "#080f0f" }}>
      <div ref={mapContainer} style={{ position: "absolute", top: 0, left: 0, right: 0, bottom: 0, width: "100%", height: "100%" }} />

      {/* Header */}
      <div className="absolute top-0 left-0 right-0 z-10 pointer-events-none"
           style={{ background: 'linear-gradient(180deg, rgba(8,15,15,0.95) 0%, transparent 100%)' }}>
        <div className="flex items-center justify-between px-4 py-3">
          <div className="pointer-events-auto">
            <h1 className="font-display text-text text-lg leading-none brand-text">Embodied Carbon Observatory // dexdogs</h1>
            <p className="font-mono text-muted text-xs mt-1 tracking-wider uppercase brand-text">US Building Materials · Temporal GWP Analysis</p>
          </div>
          <div className="font-mono text-xs text-muted">
            {plantCount > 0 && <span><span className="text-teal">{plantCount.toLocaleString()}</span> plants</span>}
          </div>
        </div>
      </div>

      {/* Search */}
      <div className="absolute top-16 left-4 z-10 w-72">
        <SearchBar onResult={handleSearchResult} />
      </div>

      {/* Filters */}
      <div className="absolute top-16 left-80 z-10">
        <FilterBar
          selectedCategory={selectedCategory}
          selectedState={selectedState}
          onCategoryChange={setSelectedCategory}
          onStateChange={setSelectedState}
        />
      </div>

      {/* Legend and Info */}
      <div className="absolute bottom-8 right-4 z-10">
        <Legend />
      </div>
      <div className="absolute bottom-8 left-4 z-10 space-y-3">
        <MapSummary
          total={plantCount}
          indexed={indexedCount}
          accessOnRequest={requestCount}
          singleEpd={singleEpdCount}
        />
        <InfoFeedback />
      </div>

      {/* Plant panel */}
      {selectedPlant && (
        <div className="absolute top-0 right-0 bottom-0 z-20 w-96 fade-in"
             style={{ background: 'rgba(8,15,15,0.97)', borderLeft: '1px solid var(--border)' }}>
          <PlantPanel plant={selectedPlant} onClose={() => { setSelectedPlant(null); map.current?.setFilter('plants-selected', ['==', ['get', 'id'], '']) }} />
        </div>
      )}
    </main>
  )
}
