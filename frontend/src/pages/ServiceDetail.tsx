import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { LogStream } from '../components/LogStream'
import { AnomalyCard } from '../components/AnomalyCard'
import { StatusDot } from '../components/StatusDot'
import { analyticsApi, anomaliesApi } from '../lib/api'
import type { TimelinePoint, Anomaly, ServiceMetrics } from '../types'

function shortHour(hour: unknown) {
  if (typeof hour !== 'string') return ''
  return new Date(hour).getHours() + ':00'
}

export default function ServiceDetail() {
  const { serviceName } = useParams<{ serviceName: string }>()
  const navigate = useNavigate()
  const name = serviceName ?? ''

  const { data: timeline } = useQuery<TimelinePoint[]>({
    queryKey: ['timeline', name],
    queryFn: () =>
      analyticsApi.serviceTimeline(name).then((r) => r.data),
    staleTime: 55_000,
    refetchInterval: 60_000,
    enabled: !!name,
  })

  const { data: anomalies, refetch } = useQuery<Anomaly[]>({
    queryKey: ['anomalies', name],
    queryFn: () =>
      anomaliesApi.list({ service: name, limit: 20 }).then((r) => r.data),
    refetchInterval: 15_000,
    staleTime: 12_000,
    enabled: !!name,
  })

  const { data: services } = useQuery<ServiceMetrics[]>({
    queryKey: ['analytics', 'services'],
    queryFn: () => analyticsApi.services().then((r) => r.data),
    staleTime: 25_000,
  })

  const svc = services?.find((s) => s.service_name === name)
  const chartData = timeline ?? []

  return (
    <div className="flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-center gap-3">
        <button
          onClick={() => navigate('/dashboard')}
          className="text-slate-400 hover:text-slate-200 transition-colors text-sm font-mono"
        >
          ← Dashboard
        </button>
        <div className="flex items-center gap-2">
          <StatusDot status={svc?.health_status ?? 'healthy'} />
          <h1 className="text-lg font-mono font-bold text-slate-200">{name}</h1>
        </div>
      </div>

      {/* Charts */}
      {chartData.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="bg-slate-800 rounded-lg p-4">
            <h3 className="text-xs font-mono text-slate-400 uppercase mb-3">
              24h Log Volume
            </h3>
            <ResponsiveContainer width="100%" height={180}>
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="volGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#06b6d4" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="hour" tickFormatter={shortHour} tick={{ fontSize: 10, fill: '#64748b' }} />
                <YAxis tick={{ fontSize: 10, fill: '#64748b' }} width={35} />
                <Tooltip
                  contentStyle={{ background: '#1e293b', border: '1px solid #334155', fontSize: 11 }}
                  labelFormatter={shortHour}
                />
                <Area type="monotone" dataKey="total_logs" stroke="#06b6d4" fill="url(#volGrad)" strokeWidth={1.5} />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-slate-800 rounded-lg p-4">
            <h3 className="text-xs font-mono text-slate-400 uppercase mb-3">
              24h Error Rate
            </h3>
            <ResponsiveContainer width="100%" height={180}>
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="errGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="hour" tickFormatter={shortHour} tick={{ fontSize: 10, fill: '#64748b' }} />
                <YAxis tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`} tick={{ fontSize: 10, fill: '#64748b' }} width={40} />
                <Tooltip
                  contentStyle={{ background: '#1e293b', border: '1px solid #334155', fontSize: 11 }}
                  labelFormatter={shortHour}
                  formatter={(v: unknown) => [`${(Number(v) * 100).toFixed(2)}%`, 'Error Rate']}
                />
                <Area type="monotone" dataKey="error_rate" stroke="#ef4444" fill="url(#errGrad)" strokeWidth={1.5} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Anomalies */}
      {(anomalies?.length ?? 0) > 0 && (
        <div>
          <h2 className="text-xs font-mono text-slate-400 uppercase tracking-wider mb-2">
            Anomalies
          </h2>
          <div className="flex flex-col gap-2">
            {(anomalies ?? []).map((a) => (
              <AnomalyCard key={a.id} anomaly={a} onUpdate={() => refetch()} />
            ))}
          </div>
        </div>
      )}

      {/* Log stream */}
      <div>
        <h2 className="text-xs font-mono text-slate-400 uppercase tracking-wider mb-2">
          Live Logs
        </h2>
        <LogStream service={name} maxHeight="400px" />
      </div>
    </div>
  )
}
