'use client'

import { useState } from 'react'

export default function InfoFeedback() {
  const [activeTab, setActiveTab] = useState<'info' | 'feedback' | null>(null)

  return (
    <div className="flex flex-col gap-2">
      {/* Info Button */}
      <button
        onClick={() => setActiveTab(activeTab === 'info' ? null : 'info')}
        className="flex items-center gap-2 px-3 py-2 text-xs font-mono rounded transition-colors"
        style={{
          background: activeTab === 'info' ? 'var(--teal)' : 'var(--surface)',
          color: activeTab === 'info' ? 'var(--bg)' : 'var(--text)',
          border: '1px solid var(--border)',
        }}
      >
        <span>ℹ</span> Info
      </button>

      {/* Feedback Button */}
      <button
        onClick={() => setActiveTab(activeTab === 'feedback' ? null : 'feedback')}
        className="flex items-center gap-2 px-3 py-2 text-xs font-mono rounded transition-colors"
        style={{
          background: activeTab === 'feedback' ? 'var(--teal)' : 'var(--surface)',
          color: activeTab === 'feedback' ? 'var(--bg)' : 'var(--text)',
          border: '1px solid var(--border)',
        }}
      >
        <span>✉</span> Feedback
      </button>

      {/* Info Panel */}
      {activeTab === 'info' && (
        <div
          className="p-3 rounded text-xs fade-in"
          style={{
            background: 'var(--panel)',
            border: '1px solid var(--border)',
            maxWidth: '280px',
          }}
        >
          <p className="font-mono text-muted mb-2 tracking-wide uppercase text-xxs">About</p>
          <p className="text-text leading-relaxed mb-3">
            Embodied Carbon Observatory maps EPA eGRID grid decarbonization vs. manufacturing process improvements in US concrete plants.
          </p>
          <a
            href="https://dexdogs.earth"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block px-2 py-1 rounded text-xs font-mono transition-colors"
            style={{
              background: 'rgba(0,229,200,0.1)',
              color: 'var(--teal)',
              border: '1px solid var(--teal)',
              textDecoration: 'none',
            }}
            onMouseEnter={(e) => {
              (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.2)'
            }}
            onMouseLeave={(e) => {
              (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.1)'
            }}
          >
            dexdogs.earth →
          </a>
        </div>
      )}

      {/* Feedback Panel */}
      {activeTab === 'feedback' && (
        <div
          className="p-3 rounded text-xs fade-in"
          style={{
            background: 'var(--panel)',
            border: '1px solid var(--border)',
            maxWidth: '280px',
          }}
        >
          <p className="font-mono text-muted mb-2 tracking-wide uppercase text-xxs">Send Feedback</p>
          <p className="text-text leading-relaxed mb-3">
            Have questions or ideas? Get in touch with the team.
          </p>
          <div className="flex flex-col gap-2">
            <a
              href="mailto:ankur@dexdogs.earth"
              className="inline-block px-2 py-1 rounded text-xs font-mono transition-colors"
              style={{
                background: 'rgba(0,229,200,0.1)',
                color: 'var(--teal)',
                border: '1px solid var(--teal)',
                textDecoration: 'none',
              }}
              onMouseEnter={(e) => {
                (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.2)'
              }}
              onMouseLeave={(e) => {
                (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.1)'
              }}
            >
              Feedback →
            </a>
            <a
              href="mailto:ankur@dexdogs.earth?subject=Request%20Access%20to%20Plant%20Data&body=Hi%20Ankur%2C%0A%0AI%20would%20like%20to%20request%20access%20to%20indexed%20plant%20data%20in%20the%20Embodied%20Carbon%20Observatory."
              className="inline-block px-2 py-1 rounded text-xs font-mono transition-colors"
              style={{
                background: 'rgba(0,229,200,0.1)',
                color: 'var(--teal)',
                border: '1px solid var(--teal)',
                textDecoration: 'none',
              }}
              onMouseEnter={(e) => {
                (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.2)'
              }}
              onMouseLeave={(e) => {
                (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.1)'
              }}
            >
              Request Access →
            </a>
          </div>
        </div>
      )}
    </div>
  )
}
