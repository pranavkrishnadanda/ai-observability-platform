import type { ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { analyticsApi } from '../lib/api'
import type { AnalyticsOverview } from '../types'

function delta(current: number, previous: number) {
  if (previous === 0) return null
  const pct = ((current - previous) / previous) * 100
  return { pct: Math.abs(pct).toFixed(1), up: pct >= 0 }
}

function StatCard({
  label,
  value,
  sub,
  accent,
}: {
  label: string
  value: string | number
  sub?: ReactNode
  accent?: string
}) {
  return (
    <div className="bg-slate-800 rounded-lg p-4 flex flex-col gap-1 min-w-0">
      <span className="text-slate-400 text-xs uppercase tracking-wider">{label}</span>
      <span className={`text-2xl font-bold font-mono ${accent ?? 'text-slate-100'}`}>
        {value}
      </span>
      {sub && <span className="text-xs text-slate-500">{sub}</span>}
    </div>
  )
}

function healthColor(score: number) {
  if (score >= 80) return 'text-emerald-400'
  if (score >= 50) return 'text-yellow-400'
  return 'text-red-400'
}

export function MetricsBar() {
  const { data } = useQuery<AnalyticsOverview>({
    queryKey: ['analytics', 'overview'],
    queryFn: () => analyticsApi.overview().then((r) => r.data),
    staleTime: 55_000,
    refetchInterval: 60_000,
  })

  if (!data) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="bg-slate-800 rounded-lg p-4 h-20 animate-pulse" />
        ))}
      </div>
    )
  }

  const logDelta = delta(data.total_logs_today, data.total_logs_yesterday)
  const errDelta = delta(data.error_rate_today, data.error_rate_yesterday)
  const score = Math.round(data.system_health_score)

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
      <StatCard
        label="Logs Today"
        value={data.total_logs_today.toLocaleString()}
        sub={
          logDelta && (
            <span className={logDelta.up ? 'text-emerald-400' : 'text-red-400'}>
              {logDelta.up ? '▲' : '▼'} {logDelta.pct}% vs yesterday
            </span>
          )
        }
      />
      <StatCard
        label="Active Anomalies"
        value={data.active_anomalies}
        accent={data.active_anomalies > 0 ? 'text-red-400' : 'text-emerald-400'}
        sub={data.active_anomalies > 0 ? 'Needs attention' : 'All clear'}
      />
      <StatCard
        label="Alerts Today"
        value={data.alerts_sent_today}
        sub={
          errDelta && (
            <span className={errDelta.up ? 'text-red-400' : 'text-emerald-400'}>
              Error rate: {(data.error_rate_today * 100).toFixed(1)}%
            </span>
          )
        }
      />
      <StatCard
        label="Health Score"
        value={`${score}/100`}
        accent={healthColor(score)}
        sub={score >= 80 ? 'Nominal' : score >= 50 ? 'Degraded' : 'Critical'}
      />
    </div>
  )
}
