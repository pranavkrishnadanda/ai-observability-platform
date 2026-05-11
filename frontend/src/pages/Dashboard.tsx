import { useQuery } from '@tanstack/react-query'
import { MetricsBar } from '../components/MetricsBar'
import { LogStream } from '../components/LogStream'
import { AnomalyCard } from '../components/AnomalyCard'
import { ServiceHealthGrid } from '../components/ServiceHealthGrid'
import { anomaliesApi } from '../lib/api'
import type { Anomaly } from '../types'

export default function Dashboard() {
  const { data: anomalies, refetch } = useQuery<Anomaly[]>({
    queryKey: ['anomalies', 'active'],
    queryFn: () =>
      anomaliesApi.list({ status: 'active', limit: 20 }).then((r) => r.data),
    refetchInterval: 10_000,
    staleTime: 8_000,
  })

  const active = anomalies ?? []

  return (
    <div className="flex flex-col gap-4">
      <MetricsBar />

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
        {/* Log stream — 60% */}
        <div className="lg:col-span-3">
          <h2 className="text-xs font-mono text-slate-400 uppercase tracking-wider mb-2">
            Live Log Stream
          </h2>
          <LogStream service="all" maxHeight="560px" />
        </div>

        {/* Anomaly panel — 40% */}
        <div className="lg:col-span-2 flex flex-col gap-2">
          <div className="flex items-center justify-between">
            <h2 className="text-xs font-mono text-slate-400 uppercase tracking-wider">
              Active Anomalies
              {active.length > 0 && (
                <span className="ml-2 bg-red-800 text-red-200 rounded-full px-2 py-0.5 text-xs">
                  {active.length}
                </span>
              )}
            </h2>
            <button
              onClick={() => refetch()}
              className="text-xs text-slate-500 hover:text-slate-300 transition-colors"
            >
              ↻
            </button>
          </div>
          <div className="flex flex-col gap-2 overflow-y-auto" style={{ maxHeight: '580px' }}>
            {active.length === 0 ? (
              <div className="bg-slate-800 rounded-lg p-6 text-center text-slate-500 text-sm font-mono">
                No active anomalies
              </div>
            ) : (
              active.map((a) => (
                <AnomalyCard key={a.id} anomaly={a} onUpdate={() => refetch()} />
              ))
            )}
          </div>
        </div>
      </div>

      <div>
        <h2 className="text-xs font-mono text-slate-400 uppercase tracking-wider mb-2">
          Service Health
        </h2>
        <ServiceHealthGrid />
      </div>
    </div>
  )
}
