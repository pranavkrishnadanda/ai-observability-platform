import { useState } from 'react'
import { anomaliesApi } from '../lib/api'
import type { Anomaly } from '../types'

function timeAgo(ts: string) {
  const diff = Date.now() - new Date(ts).getTime()
  const mins = Math.floor(diff / 60000)
  if (mins < 1) return 'just now'
  if (mins < 60) return `${mins} minute${mins !== 1 ? 's' : ''} ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs} hour${hrs !== 1 ? 's' : ''} ago`
  return `${Math.floor(hrs / 24)} days ago`
}

function severityBorder(score: number) {
  if (score >= 0.7) return 'border-l-red-500'
  if (score >= 0.3) return 'border-l-orange-500'
  return 'border-l-yellow-500'
}

interface AnomalyCardProps {
  anomaly: Anomaly
  onUpdate: () => void
}

export function AnomalyCard({ anomaly, onUpdate }: AnomalyCardProps) {
  const [expanded, setExpanded] = useState(false)
  const [loading, setLoading] = useState(false)
  const isResolved = anomaly.status !== 'active'

  async function handleAction(action: 'acknowledge' | 'resolve') {
    setLoading(true)
    try {
      if (action === 'acknowledge') await anomaliesApi.acknowledge(anomaly.id)
      else await anomaliesApi.resolve(anomaly.id)
      onUpdate()
    } catch {
      // show nothing — onUpdate will re-fetch
    } finally {
      setLoading(false)
    }
  }

  const analysisRaw = anomaly.claude_analysis ?? ''
  let analysis = analysisRaw
  try {
    const parsed = JSON.parse(analysisRaw)
    analysis = parsed?.root_cause ?? analysisRaw
  } catch { /* not JSON, use as-is */ }
  const shortAnalysis = analysis.length > 180 ? analysis.slice(0, 180) + '…' : analysis

  return (
    <div
      className={`bg-slate-800 rounded-lg border-l-4 ${severityBorder(anomaly.severity_score)} p-4 ${isResolved ? 'opacity-60' : ''}`}
    >
      <div className="flex items-start justify-between gap-2 mb-2">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="font-mono text-sm font-semibold text-slate-200">
            {anomaly.service_name}
          </span>
          <span className="text-xs bg-slate-700 text-slate-300 px-2 py-0.5 rounded font-mono">
            {anomaly.anomaly_type}
          </span>
          {anomaly.status !== 'active' && (
            <span className="text-xs bg-slate-600 text-slate-400 px-2 py-0.5 rounded capitalize">
              {anomaly.status}
            </span>
          )}
        </div>
        <span className="text-xs text-slate-500 shrink-0">{timeAgo(anomaly.detected_at)}</span>
      </div>

      <div className="flex items-center gap-4 mb-3 font-mono">
        {anomaly.deviation_pct != null ? (
          <span
            className={`text-2xl font-bold ${
              anomaly.deviation_pct >= 100 ? 'text-red-400' : 'text-orange-400'
            }`}
          >
            +{anomaly.deviation_pct.toFixed(0)}%
          </span>
        ) : (
          <span className="text-2xl font-bold text-orange-400">new pattern</span>
        )}
        <div className="text-xs text-slate-500">
          {anomaly.baseline_value != null && <div>baseline {anomaly.baseline_value.toFixed(2)}</div>}
          {anomaly.observed_value != null && <div>observed {anomaly.observed_value.toFixed(2)}</div>}
        </div>
        <div className="text-xs text-slate-500">
          score {anomaly.severity_score.toFixed(2)}
        </div>
      </div>

      {analysis && (
        <p className="text-xs text-slate-400 mb-3 leading-relaxed">
          {expanded ? analysis : shortAnalysis}
          {analysis.length > 180 && (
            <button
              onClick={() => setExpanded((e) => !e)}
              className="ml-1 text-cyan-400 hover:text-cyan-300"
            >
              {expanded ? 'less' : 'more'}
            </button>
          )}
        </p>
      )}

      {!isResolved && (
        <div className="flex gap-2">
          <button
            disabled={loading || anomaly.status === 'acknowledged'}
            onClick={() => handleAction('acknowledge')}
            className="px-3 py-1 text-xs rounded bg-slate-700 hover:bg-slate-600 text-slate-300 disabled:opacity-40 transition-colors"
          >
            Acknowledge
          </button>
          <button
            disabled={loading}
            onClick={() => handleAction('resolve')}
            className="px-3 py-1 text-xs rounded bg-emerald-900/60 hover:bg-emerald-900 text-emerald-300 disabled:opacity-40 transition-colors"
          >
            Resolve
          </button>
        </div>
      )}
    </div>
  )
}
