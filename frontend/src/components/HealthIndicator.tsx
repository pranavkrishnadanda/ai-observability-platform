import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { healthApi } from '../lib/api'
import { StatusDot } from './StatusDot'
import type { HealthStatus } from '../types'

type ServiceKey = 'postgres' | 'redis' | 'kafka'
const SERVICES: ServiceKey[] = ['postgres', 'redis', 'kafka']

function statusFromValue(v: string | undefined): 'healthy' | 'degraded' | 'critical' {
  if (!v) return 'critical'
  const lower = v.toLowerCase()
  if (lower === 'ok' || lower === 'healthy') return 'healthy'
  if (lower === 'degraded') return 'degraded'
  return 'critical'
}

export function HealthIndicator() {
  const [open, setOpen] = useState(false)
  const { data } = useQuery<HealthStatus>({
    queryKey: ['health'],
    queryFn: () => healthApi.check().then((r) => r.data),
    refetchInterval: 15_000,
    staleTime: 10_000,
  })

  return (
    <div className="relative">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 px-2 py-1 rounded hover:bg-slate-700 transition-colors"
        title="System health"
      >
        {SERVICES.map((svc) => (
          <StatusDot key={svc} status={data ? statusFromValue(data[svc]) : 'connecting'} />
        ))}
      </button>
      {open && (
        <div className="absolute right-0 top-8 bg-slate-800 border border-slate-600 rounded-lg p-3 text-xs font-mono w-40 z-50 shadow-xl">
          {SERVICES.map((svc) => (
            <div key={svc} className="flex items-center justify-between py-1">
              <span className="text-slate-400 capitalize">{svc}</span>
              <div className="flex items-center gap-1.5">
                <StatusDot status={data ? statusFromValue(data[svc]) : 'connecting'} />
                <span className="text-slate-500">
                  {data ? data[svc] : '…'}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
