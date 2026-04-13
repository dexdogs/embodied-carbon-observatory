'use client'

import { useEffect, useRef } from 'react'

interface Props {
  onMapReady: (map: any) => void
}

export default function Map({ onMapReady }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    let map: any

    const initMap = async () => {
      const mapboxgl = (await import('mapbox-gl')).default
      await import('mapbox-gl/dist/mapbox-gl.css')
      
      mapboxgl.accessToken = 'MAPBOX_TOKEN_REMOVED'

      map = new mapboxgl.Map({
        container: containerRef.current!,
        style: 'mapbox://styles/mapbox/dark-v11',
        center: [-95, 38],
        zoom: 4,
      })

      map.on('load', () => {
        onMapReady(map)
      })
    }

    initMap()

    return () => map?.remove()
  }, [])

  return <div ref={containerRef} className="absolute inset-0" />
}
