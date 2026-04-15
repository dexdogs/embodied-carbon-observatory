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

      {/* Request Access Button */}
      
        href="mailto:ankur@dexdogs.earth?subject=Request%20Access%20to%20Plant%20Data&body=Hi%20Ankur%2C%0A%0AI%20would%20like%20to%20request%20access%20to%20indexed%20plant%20data%20in%20the%20Embodied%20Carbon%20Observatory."
        className="flex items-center gap-2 px-3 py-2 text-xs font-mono rounded transition-colors"
        style={{
          background: 'var(--surface)',
          color: 'var(--text)',
          border: '1px solid var(--border)',
          textDecoration: 'none',
        }}
        onMouseEnter={(e: any) => {
          e.target.style.background = 'rgba(0,229,200,0.1)';
          e.target.style.color = 'var(--teal)';
        }}
        onMouseLeave={(e: any) => {
          e.target.style.background = 'var(--surface)';
          e.target.style.color = 'var(--text)';
        }}
      >
        <span>🔐</span> Request Access
      </a>

      {/* Info Panel */}
      {activeTab === 'info' && (
        <div
          className="p-3 rounded text-xs fade-in"
          style={{
            background: 'var(--panel)',
            border: '1px solid var(--border)',
            maxWidth: '320px',
            maxHeight: '70vh',
            overflowY: 'auto',
          }}
        >
          <p className="font-mono text-muted mb-3 tracking-wide uppercase text-xxs">About</p>

          <p className="text-text leading-relaxed mb-3">
            Embodied Carbon Observatory maps EPA eGRID grid decarbonization vs. manufacturing process improvements in US concrete plants.
          </p>

          <div className="mb-3" style={{ borderTop: '1px solid var(--border)', paddingTop: '10px' }}>
            <p className="font-mono mb-2" style={{ color: 'var(--teal)', fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              The grid vs. process decomposition is alpha
            </p>
            <p className="text-text leading-relaxed mb-2">
              When a concrete plant's EPD shows improving GWP, the obvious interpretation is "this company is decarbonizing." But if 80% of that improvement came from the grid getting cleaner — that's not operational excellence, that's geographic luck. The plant did nothing. A fund holding that company's bonds or equity on the basis of "carbon improvement trajectory" has been misled by a number that looks like management action but isn't.
            </p>
            <p className="font-mono mb-1" style={{ color: 'var(--muted)', fontSize: '9px', textTransform: 'uppercase' }}>The process-driven component tells you:</p>
            <ul className="mb-3" style={{ paddingLeft: '12px', listStyleType: 'disc' }}>
              {[
                'Is management making capital investments in lower-carbon inputs?',
                'Is cement substitution (SCM ratio) trending up over time?',
                'Is this a company that will continue improving when the grid plateaus?',
              ].map((item, i) => (
                <li key={i} className="text-text leading-relaxed mb-1">{item}</li>
              ))}
            </ul>
          </div>

          <div className="mb-3" style={{ borderTop: '1px solid var(--border)', paddingTop: '10px' }}>
            <p className="font-mono mb-2" style={{ color: 'var(--teal)', fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              What dexdogs has productized
            </p>
            <p className="text-text leading-relaxed mb-2">
              The facility-level scorecard — which plants are genuinely improving vs. riding grid tailwinds. Aggregated to company level, that becomes a carbon quality score that no Bloomberg terminal, no ESG rating agency, and no sell-side analyst currently publishes with this level of operational specificity.
            </p>
            <p className="text-text leading-relaxed mb-2">
              The counterfactual time series — the "what if only the grid changed" line on the chart. That's the baseline needed to strip out the macro from the micro. It's the difference between a company with genuine process innovation and one that looks green on paper because California built more solar.
            </p>
            <p className="text-text leading-relaxed">
              For a $2B materials sector fund, knowing which cement and concrete plants are genuinely decarbonizing their process — not just benefiting from renewable energy build-out — is a differentiated view that affects how you underwrite the transition risk of their bonds and the terminal value assumptions in your equity models.
            </p>
          </div>

          
            href="https://dexdogs.earth"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block px-2 py-1 rounded text-xs font-mono transition-colors mt-1"
            style={{
              background: 'rgba(0,229,200,0.1)',
              color: 'var(--teal)',
              border: '1px solid var(--teal)',
              textDecoration: 'none',
            }}
            onMouseEnter={(e) => { (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.2)' }}
            onMouseLeave={(e) => { (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.1)' }}
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
          
            href="mailto:ankur@dexdogs.earth"
            className="inline-block px-2 py-1 rounded text-xs font-mono transition-colors"
            style={{
              background: 'rgba(0,229,200,0.1)',
              color: 'var(--teal)',
              border: '1px solid var(--teal)',
              textDecoration: 'none',
            }}
            onMouseEnter={(e) => { (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.2)' }}
            onMouseLeave={(e) => { (e.target as HTMLElement).style.background = 'rgba(0,229,200,0.1)' }}
          >
            Send Feedback →
          </a>
        </div>
      )}
    </div>
  )
}
