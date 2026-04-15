'use client'

interface MapSummaryProps {
  total: number
  indexed: number
  accessOnRequest: number
  singleEpd: number
}

export default function MapSummary({ total, indexed, accessOnRequest, singleEpd }: MapSummaryProps) {
  return (
    <div className="panel p-3 fade-in" style={{ background: 'rgba(8,15,15,0.92)', minWidth: '240px' }}>
      <div className="font-mono text-muted uppercase tracking-wider mb-3" style={{ fontSize: '9px' }}>
        Demo coverage
      </div>
      <div className="space-y-2 text-sm">
        <div className="flex items-center justify-between">
          <span className="font-mono text-muted" style={{ fontSize: '11px' }}>Total plants on map</span>
          <span className="font-mono text-teal" style={{ fontSize: '11px' }}>{total.toLocaleString()}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="font-mono text-muted" style={{ fontSize: '11px' }}>Indexed for this demo</span>
          <span className="font-mono text-amber" style={{ fontSize: '11px' }}>{indexed.toLocaleString()}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="font-mono text-muted" style={{ fontSize: '11px' }}>Access upon request</span>
          <span className="font-mono text-text" style={{ fontSize: '11px' }}>{accessOnRequest.toLocaleString()}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="font-mono text-muted" style={{ fontSize: '11px' }}>Single EPD only</span>
          <span className="font-mono text-teal" style={{ fontSize: '11px' }}>{singleEpd.toLocaleString()}</span>
        </div>
      </div>
    </div>
  )
}
