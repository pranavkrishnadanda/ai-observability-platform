import { LineChart, Line, ResponsiveContainer } from 'recharts'
import { StatusDot } from './StatusDot'
import type { ServiceMetrics } from '../types'

const STATUS_BORDER: Record<ServiceMetrics['health_status'], string> = {
  healthy: 'border-green-700 bg-green-950/20',
  degraded: 'border-yellow-700 bg-yellow-950/20',
  critical: 'border-red-700 bg-red-950/20',
}

const SPARK_COLOR: Record<ServiceMetrics['health_status'], string> = {
  healthy: '#10b981',
  degraded: '#f59e0b',
  critical: '#ef4444',
}

interface ServiceCardProps {
  service: ServiceMetrics
  onClick: () => void
}

export function ServiceCard({ service, onClick }: ServiceCardProps) {
  // Generate synthetic sparkline from error_rate (12 points trending)
  const spark = Array.from({ length: 12 }, (_, i) => ({
    v: Math.max(0, service.error_rate_1h + (Math.random() - 0.5) * 0.02 * i),
  }))

  return (
    <div
      onClick={onClick}
      className={`bg-slate-800 rounded-lg p-4 border cursor-pointer hover:brightness-110 transition-all ${STATUS_BORDER[service.health_status]}`}
    >
      <div className="flex items-center gap-2 mb-3">
        <StatusDot status={service.health_status} />
        <span className="font-mono text-sm font-semibold text-slate-200 flex-1 truncate">
          {service.service_name}
        </span>
      </div>

      <div className="h-10 mb-3">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={spark}>
            <Line
              type="monotone"
              dataKey="v"
              stroke={SPARK_COLOR[service.health_status]}
              strokeWidth={1.5}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="grid grid-cols-3 gap-1 text-xs font-mono">
        <div>
          <div className="text-slate-500">Vol/1h</div>
          <div className="text-slate-300">{service.log_volume_1h.toLocaleString()}</div>
        </div>
        <div>
          <div className="text-slate-500">Err%</div>
          <div className={service.error_rate_1h > 0.05 ? 'text-red-400' : 'text-slate-300'}>
            {(service.error_rate_1h * 100).toFixed(1)}%
          </div>
        </div>
        <div>
          <div className="text-slate-500">Anom/7d</div>
          <div className={service.anomaly_count_7d > 0 ? 'text-orange-400' : 'text-slate-300'}>
            {service.anomaly_count_7d}
          </div>
        </div>
      </div>
    </div>
  )
}
