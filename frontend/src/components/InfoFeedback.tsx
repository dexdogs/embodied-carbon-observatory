'use client'

import { useState } from 'react'

export default function InfoFeedback() {
  const [activeTab, setActiveTab] = useState<'info' | 'feedback' | null>(null)

  return (
    <div className="flex flex-col gap-2">
      <button
        onClick={() => setActiveTab(activeTab === 'info' ? null : 'info')}
        className="flex items-center gap-2 px-3 py-2 text-xs font-mono rounded transition-colors"
        style={{
          background: activeTab === 'info' ? 'var(--teal)' : 'var(--surface)',
          color: activeTab === 'info' ? 'var(--bg)' : 'var(--text)',
          border: '1px solid var(--border)',
        }}
      >
        <span>&#9432;</span> Info
      </button>

      <button
        onClick={() => setActiveTab(activeTab === 'feedback' ? null : 'feedback')}
        className="flex items-center gap-2 px-3 py-2 text-xs font-mono rounded transition-colors"
        style={{
          background: activeTab === 'feedback' ? 'var(--teal)' : 'var(--surface)',
          color: activeTab === 'feedback' ? 'var(--bg)' : 'var(--text)',
          border: '1px solid var(--border)',
        }}
      >
        <span>&#9993;</span> Feedback
      </button>

      
        href="mailto:ankur@dexdogs.earth?subject=Request%20Access%20to%20Plant%20Data&body=Hi%20Ankur%2C%0A%0AI%20would%20like%20to%20request%20access%20to%20indexed%20plant%20data%20in%20the%20Embodied%20Carbon%20Observatory."
        className="flex items-center gap-2 px-3 py-2 text-xs font-mono rounded transition-colors"
        style={{
          background: 'var(--surface)',
          color: 'var(--text)',
          border: '1px solid var(--border)',
          textDecoration: 'none',
        }}
        onMouseEnter={(e: React.MouseEvent<HTMLAnchorElement>) => { e.currentTarget.style.background = 'rgba(0,229,200,0.1)'; e.currentTarget.style.color = 'var(--teal)' }}
        onMouseLeave={(e: React.MouseEvent<HTMLAnchorElement>) => { e.currentTarget.style.background = 'var(--surface)'; e.currentTarget.style.color = 'var(--text)' }}>
        <span>&#128274;</span> Request Access
      </a>

      {activeTab === 'info' && (
        <div
          className="p-3 rounded text-xs fade-in"
          style={{ background: 'var(--panel)', border: '1px solid var(--border)', maxWidth: '320px', maxHeight: '70vh', overflowY: 'auto' }}
        >
          <p className="font-mono text-muted mb-3 tracking-wide uppercase" style={{ fontSize: '9px' }}>About</p>
          <p className="text-text leading-relaxed mb-3">
            Embodied Carbon Observatory maps EPA eGRID grid decarbonization vs. manufacturing process improvements in US concrete plants.
          </p>

          <div className="mb-3" style={{ borderTop: '1px solid var(--border)', paddingTop: '10px' }}>
            <p className="font-mono mb-2" style={{ color: 'var(--teal)', fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              The grid vs. process decomposition is alpha
            </p>
            <p className="text-text leading-relaxed mb-2">
              When a concrete plant&apos;s EPD shows improving GWP, the obvious interpretation is &quot;this company is decarbonizing.&quot; But if 80% of that improvement came from the grid getting cleaner — that&apos;s not operational excellence, that&apos;s geographic luck. The plant did nothing. A fund holding that company&apos;s bonds or equity on the basis of &quot;carbon improvement trajectory&quot; has been misled by a number that looks like management action but isn&apos;t.
            </p>
            <p className="font-mono mb-1" style={{ color: 'var(--muted)', fontSize: '9px', textTransform: 'uppercase' }}>The process-driven component tells you:</p>
            <ul className="mb-3 text-text" style={{ paddingLeft: '12px', listStyleType: 'disc' }}>
              <li className="leading-relaxed mb-1">Is management making capital investments in lower-carbon inputs?</li>
              <li className="leading-relaxed mb-1">Is cement substitution (SCM ratio) trending up over time?</li>
              <li className="leading-relaxed">Is this a company that will continue improving when the grid plateaus?</li>
            </ul>
          </div>

          <div className="mb-3" style={{ borderTop: '1px solid var(--border)', paddingTop: '10px' }}>
            <p className="font-mono mb-2" style={{ color: 'var(--teal)', fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              What dexdogs has built
            </p>
            <p className="text-text leading-relaxed mb-2">
              The facility-level scorecard — which plants are genuinely improving vs. riding grid tailwinds. Aggregated to company level, that becomes a carbon quality score that no Bloomberg terminal, no ESG rating agency, and no sell-side analyst currently publishes with this level of operational specificity.
            </p>
            <p className="text-text leading-relaxed mb-2">
              The counterfactual time series — the &quot;what if only the grid changed&quot; line on the chart. That&apos;s the baseline needed to strip out the macro from the micro. It&apos;s the difference between a company with genuine process innovation and one that looks green on paper because California built more solar.
            </p>
            <p className="text-text leading-relaxed">
              For a $2B materials sector fund, knowing which cement and concrete plants are genuinely decarbonizing their process — not just benefiting from renewable energy build-out — is a differentiated view that affects how you underwrite the transition risk of their bonds and the terminal value assumptions in your equity models.
            </p>
          </div>

          
            href="https://dexdogs.earth"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block px-2 py-1 rounded text-xs font-mono mt-1"
            style={{ background: 'rgba(0,229,200,0.1)', color: 'var(--teal)', border: '1px solid var(--teal)', textDecoration: 'none' }}
            onMouseEnter={(e: React.MouseEvent<HTMLAnchorElement>) => { e.currentTarget.style.background = 'rgba(0,229,200,0.2)' }}
            onMouseLeave={(e: React.MouseEvent<HTMLAnchorElement>) => { e.currentTarget.style.background = 'rgba(0,229,200,0.1)' }}
          >
            dexdogs.earth &#8594;
          </a>
        </div>
      )}

      {activeTab === 'feedback' && (
        <div
          className="p-3 rounded text-xs fade-in"
          style={{ background: 'var(--panel)', border: '1px solid var(--border)', maxWidth: '280px' }}
        >
          <p className="font-mono text-muted mb-2 tracking-wide uppercase" style={{ fontSize: '9px' }}>Send Feedback</p>
          <p className="text-text leading-relaxed mb-3">Have questions or ideas? Get in touch with the team.</p>
          
            href="mailto:ankur@dexdogs.earth"
            className="inline-block px-2 py-1 rounded text-xs font-mono"
            style={{ background: 'rgba(0,229,200,0.1)', color: 'var(--teal)', border: '1px solid var(--teal)', textDecoration: 'none' }}
            onMouseEnter={(e: React.MouseEvent<HTMLAnchorElement>) => { e.currentTarget.style.background = 'rgba(0,229,200,0.2)' }}
            onMouseLeave={(e: React.MouseEvent<HTMLAnchorElement>) => { e.currentTarget.style.background = 'rgba(0,229,200,0.1)' }}
          >
            Send Feedback &#8594;
          </a>
        </div>
      )}
    </div>
  )
}
