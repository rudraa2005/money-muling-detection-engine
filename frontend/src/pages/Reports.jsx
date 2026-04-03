import Navbar from '../components/Navbar'
import { useAnalysis } from '../context/AnalysisContext'
import { useNavigate } from 'react-router-dom'
import { buildStrictExportPayload } from '../utils/jsonExport'

function deriveRiskBand(score) {
  if (score >= 85) return 'Critical'
  if (score >= 70) return 'High'
  if (score >= 50) return 'Medium'
  return 'Watch'
}

function inferCurrencySymbol(analysis) {
  const explicit =
    analysis?.summary?.currency_symbol ||
    analysis?.currency_symbol ||
    analysis?.meta?.currency_symbol
  const symbol = typeof explicit === 'string' ? explicit.trim() : ''
  return symbol && symbol !== '?' ? symbol : '?'
}

function formatAmount(value, currencySymbol = '') {
  const num = Number(value)
  if (!Number.isFinite(num)) return '--'
  const formatted = num.toLocaleString(undefined, { maximumFractionDigits: 2 })
  const prefix = (!currencySymbol || currencySymbol.includes('?') || /[^\x00-\x7F]/.test(currencySymbol)) ? 'INR ' : currencySymbol
  return `${prefix}${formatted}`
}

function computeTotalFlaggedVolume(analysis) {
  if (!analysis) return null

  const suspiciousIds = new Set((analysis.suspicious_accounts || []).map((a) => String(a.account_id)))
  const edges = analysis?.graph_data?.edges || []

  if (suspiciousIds.size > 0 && edges.length > 0) {
    let total = 0
    for (const edge of edges) {
      const source = String(edge?.source ?? '')
      const target = String(edge?.target ?? '')
      if (!suspiciousIds.has(source) && !suspiciousIds.has(target)) continue
      const amount = Number(edge?.amount)
      if (Number.isFinite(amount)) total += amount
    }
    return total
  }

  let fallback = 0
  for (const account of analysis.suspicious_accounts || []) {
    const amt = Number(account?.score_breakdown?.total_amount)
    if (Number.isFinite(amt)) fallback += amt
  }
  return fallback > 0 ? fallback : null
}

export default function Reports() {
  const { analysis } = useAnalysis()
  const navigate = useNavigate()
  const currencySymbol = inferCurrencySymbol(analysis)
  const totalFlaggedVolume = computeTotalFlaggedVolume(analysis)

  const suspiciousAccounts = analysis?.suspicious_accounts ?? []
  const accounts = suspiciousAccounts.length
    ? suspiciousAccounts.map((a) => {
        const type = deriveRiskBand(a.suspicion_score)
        const volume = Number.isFinite(Number(a.score_breakdown?.total_amount))
          ? formatAmount(a.score_breakdown.total_amount, currencySymbol)
          : '--'
        const lastEvent = (a.risk_timeline || []).slice(-1)[0]
        const date = lastEvent?.timestamp ? new Date(lastEvent.timestamp).toLocaleDateString() : 'N/A'
        const time = lastEvent?.timestamp ? new Date(lastEvent.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '--'
        return {
          id: a.account_id,
          initials: a.account_id.slice(0, 2).toUpperCase(),
          score: Math.round(a.suspicion_score),
          type,
          ring: a.ring_id === 'RING_NONE' ? 'Unassigned' : a.ring_id,
          volume,
          date,
          time,
        }
      })
    : [
        { id: 'ACCT-9928-X', initials: 'JD', score: 98, type: 'Critical', ring: 'Ring #402', volume: '$1,240,500.00', date: 'Oct 24, 2023', time: '14:30' },
        { id: 'ACCT-8821-B', initials: 'MK', score: 92, type: 'High', ring: 'Cluster 5', volume: '$840,200.00', date: 'Oct 24, 2023', time: '12:15' },
        { id: 'ACCT-7734-Q', initials: 'TS', score: 89, type: 'High', ring: 'Beta Ring', volume: '$45,100.00', date: 'Oct 23, 2023', time: '09:45' },
        { id: 'ACCT-3319-M', initials: 'LA', score: 85, type: 'Medium', ring: 'Lone Actor', volume: '$12,900.00', date: 'Oct 22, 2023', time: '18:20' },
        { id: 'ACCT-1102-Z', initials: 'RP', score: 76, type: 'Medium', ring: 'Cluster 5', volume: '$9,500.00', date: 'Oct 22, 2023', time: '10:10' },
        { id: 'ACCT-0045-P', initials: 'XY', score: 64, type: 'Watch', ring: 'Unassigned', volume: '$2,100.00', date: 'Oct 21, 2023', time: '16:00' },
      ]

  return (
    <div className="bg-background-dark text-text-muted font-display min-h-screen flex flex-col overflow-x-hidden">
      <Navbar />

      <main className="flex-1 w-full max-w-[1600px] mx-auto p-6 lg:p-12 flex flex-col gap-10 relative mt-24">
        {/* Edge Gradients */}
        <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-accent-blue via-accent-purple to-accent-red opacity-50 pointer-events-none"></div>

        <div className="absolute top-0 left-1/2 -translate-x-1/2 w-[600px] h-[300px] bg-accent-blue/5 blur-[120px] pointer-events-none z-0"></div>

        <header className="flex flex-col md:flex-row md:items-end justify-between gap-6 relative z-10">
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-accent-blue text-xs font-bold uppercase tracking-widest font-body">
              <span className="material-symbols-outlined text-[18px]">verified_user</span>
              <span>Fraud Intelligence</span>
            </div>
            <h2 className="text-5xl font-medium text-white tracking-tight leading-none font-display">
              Suspicious Activity
            </h2>
            <p className="text-text-muted max-w-lg text-sm leading-relaxed font-body">
              {analysis
                ? 'High-risk entities identified from the latest backend analysis run.'
                : 'High-risk entities from a sample dataset. Upload a CSV on Home to run a fresh analysis.'}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            {analysis && (
              <button
                onClick={() => {
                  const strictPayload = buildStrictExportPayload(analysis)
                  const blob = new Blob([JSON.stringify(strictPayload, null, 2)], { type: 'application/json' })
                  const url = URL.createObjectURL(blob)
                  const a = document.createElement('a')
                  a.href = url
                  a.download = 'fraud_detection_output.json'
                  document.body.appendChild(a)
                  a.click()
                  a.remove()
                  URL.revokeObjectURL(url)
                }}
                className="flex items-center gap-2 px-6 py-3 rounded-xl bg-card-dark text-text-muted border border-white/10 hover:bg-white/5 hover:text-white transition-all text-xs font-bold font-body"
              >
                <span className="material-symbols-outlined text-[18px]">data_object</span>
                Download JSON
              </button>
            )}
            <button
              onClick={() => {
                const headers = ['Account ID', 'Initials', 'Score', 'Type', 'Ring', 'Volume', 'Date', 'Time']
                const csvContent = 'data:text/csv;charset=utf-8,'
                  + headers.join(',') + '\n'
                  + accounts.map(e => [e.id, e.initials, e.score, e.type, e.ring, e.volume.replace(/,/g, ''), e.date, e.time].join(',')).join('\n')
                const encodedUri = encodeURI(csvContent)
                const link = document.createElement('a')
                link.setAttribute('href', encodedUri)
                link.setAttribute('download', 'fraud_report.csv')
                document.body.appendChild(link)
                link.click()
                link.remove()
              }}
              className="flex items-center gap-2 px-6 py-3 rounded-xl bg-white text-black hover:bg-neutral-200 transition-all text-xs font-bold shadow-lg shadow-white/5 font-body">
              <span className="material-symbols-outlined text-[18px]">download</span>
              Export Report
            </button>
          </div>
        </header>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 relative z-10">
          <div className="p-6 rounded-3xl border border-white/5 bg-card-dark hover:border-accent-blue/30 transition-colors group">
            <p className="text-text-muted text-[11px] font-bold uppercase tracking-widest mb-2 font-body">Total Flagged Volume</p>
            <p className="text-4xl font-medium text-white tracking-tight font-display">
              {analysis
                ? (totalFlaggedVolume !== null ? formatAmount(totalFlaggedVolume, currencySymbol) : '--')
                : '$4.2M'}
              <span className="text-xs font-bold text-accent-blue ml-2 bg-accent-blue/10 px-1.5 py-0.5 rounded font-body">
                Live
              </span>
            </p>
          </div>
          <div className="p-6 rounded-3xl border border-white/5 bg-card-dark hover:border-accent-purple/30 transition-colors group">
            <p className="text-text-muted text-[11px] font-bold uppercase tracking-widest mb-2 font-body">Active Fraud Rings</p>
            <p className="text-4xl font-medium text-white tracking-tight font-display">
              {analysis?.summary?.fraud_rings_detected ?? 8}
              <span className="text-xs font-bold text-accent-purple ml-2 bg-accent-purple/10 px-1.5 py-0.5 rounded font-body">Detected</span>
            </p>
          </div>
          <div className="p-6 rounded-3xl border border-white/5 bg-card-dark hover:border-accent-red/30 transition-colors group">
            <p className="text-text-muted text-[11px] font-bold uppercase tracking-widest mb-2 font-body">Avg Risk Score</p>
            <p className="text-4xl font-medium text-white tracking-tight font-display">
              {accounts.length
                ? Math.round(
                    accounts.reduce((sum, a) => sum + (a.score || 0), 0) / accounts.length,
                  )
                : 87}
              <span className="text-xs font-bold text-text-muted ml-2 font-body">/ 100</span>
            </p>
          </div>
        </div>

        <div className="w-full bg-card-dark border border-white/5 rounded-3xl overflow-hidden shadow-2xl relative z-10 flex flex-col">
          <div className="p-6 border-b border-white/5 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <span className="text-text-muted text-xs font-medium font-body">
                Showing <span className="text-white">{accounts.length}</span> high-risk accounts
              </span>
            </div>
            <div className="flex items-center gap-2">
              <button className="p-2 rounded-lg hover:bg-white/5 text-text-muted hover:text-white transition-colors">
                <span className="material-symbols-outlined text-[20px]">refresh</span>
              </button>
              <button className="p-2 rounded-lg hover:bg-white/5 text-text-muted hover:text-white transition-colors">
                <span className="material-symbols-outlined text-[20px]">more_vert</span>
              </button>
            </div>
          </div>

            <div className="overflow-x-auto w-full">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="text-[11px] uppercase text-text-muted border-b border-white/5 bg-white/[0.02] font-body">
                  <th className="py-5 px-8 font-semibold tracking-wider w-[200px]">Account ID</th>
                  <th className="py-5 px-6 font-semibold tracking-wider w-[280px]">Risk Score</th>
                  <th className="py-5 px-6 font-semibold tracking-wider w-[200px]">Ring Association</th>
                  <th className="py-5 px-6 font-semibold tracking-wider w-[200px] text-right">Total Tx Volume</th>
                  <th className="py-5 px-6 font-semibold tracking-wider w-[220px]">Last Flagged Date</th>
                  <th className="py-5 px-6 font-semibold tracking-wider w-[80px]"></th>
                </tr>
              </thead>
              <tbody className="text-sm">
                {accounts.map((account, idx) => (
                  <tr key={idx} className="border-b border-white/5 hover:bg-white/[0.02] transition-colors group">
                    <td className="py-4 px-8">
                      <div className="flex items-center gap-4">
                        <div className="size-8 rounded-lg bg-white/5 flex items-center justify-center text-text-muted font-bold text-[10px] border border-white/5 font-technical">
                          {account.initials}
                        </div>
                        <span
                          className="text-white font-technical text-xs font-medium tracking-wide group-hover:text-accent-blue transition-colors cursor-pointer"
                          onClick={() => navigate(`/network-graph?account=${encodeURIComponent(account.id)}`)}
                        >
                          {account.id}
                        </span>
                      </div>
                    </td>
                    <td className="py-4 px-6">
                      <div className="flex flex-col gap-1.5">
                        <div className="flex justify-between text-[10px] font-bold font-body">
                          <span className={account.type === 'Critical' ? 'text-accent-red' : (account.type === 'High' ? 'text-accent-purple' : 'text-accent-blue')}>{account.type}</span>
                          <span className="text-white font-technical">{account.score}/100</span>
                        </div>
                        <div className="w-full bg-white/5 rounded-full h-1 overflow-hidden">
                          <div className={`h-full rounded-full ${account.type === 'Critical' ? 'bg-accent-red' : (account.type === 'High' ? 'bg-accent-purple' : 'bg-accent-blue')}`} style={{ width: `${account.score}%` }}></div>
                        </div>
                      </div>
                    </td>
                    <td className="py-4 px-6">
                      <span className={`inline-flex items-center px-2.5 py-1 rounded-md border text-[10px] font-bold uppercase tracking-wider font-body ${account.ring === 'Unassigned'
                          ? 'bg-white/5 border-white/10 text-text-muted'
                          : 'bg-accent-purple/10 border-accent-purple/20 text-accent-purple'
                        }`}>
                        {account.ring}
                      </span>
                    </td>
                    <td className="py-4 px-6 text-right font-technical text-xs text-text-muted font-medium group-hover:text-white transition-colors">
                      {account.volume}
                    </td>
                    <td className="py-4 px-6 text-text-muted text-xs font-body">
                      {account.date} <span className="text-white/20 px-1">|</span> {account.time}
                    </td>
                    <td className="py-4 px-6 text-right">
                      <button className="text-text-muted hover:text-white transition-colors p-2 rounded hover:bg-white/5">
                        <span className="material-symbols-outlined text-[20px]">more_horiz</span>
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="p-4 bg-white/[0.01] flex items-center justify-between border-t border-white/5">
            <div className="flex items-center gap-2 text-xs text-text-muted font-body">
              <span>Rows per page:</span>
              <select className="bg-card-dark border border-white/10 rounded px-2 py-1 text-white focus:ring-1 focus:ring-white/20 focus:outline-none">
                <option>10</option>
                <option>25</option>
                <option>50</option>
              </select>
            </div>
            <div className="flex items-center gap-1">
              <button className="size-8 flex items-center justify-center rounded-lg hover:bg-white/5 text-text-muted hover:text-white transition-colors">
                <span className="material-symbols-outlined text-[18px]">chevron_left</span>
              </button>
              <button className="size-8 flex items-center justify-center rounded-lg bg-white/10 text-white font-bold transition-colors border border-white/10 text-xs font-technical">1</button>
              <button className="size-8 flex items-center justify-center rounded-lg hover:bg-white/5 text-text-muted hover:text-white transition-colors text-xs font-technical">2</button>
              <button className="size-8 flex items-center justify-center rounded-lg hover:bg-white/5 text-text-muted hover:text-white transition-colors text-xs font-technical">3</button>
              <span className="text-text-muted px-1 text-xs">...</span>
              <button className="size-8 flex items-center justify-center rounded-lg hover:bg-white/5 text-text-muted hover:text-white transition-colors">
                <span className="material-symbols-outlined text-[18px]">chevron_right</span>
              </button>
            </div>
          </div>
        </div>
      </main>
    </div >
  )
}





