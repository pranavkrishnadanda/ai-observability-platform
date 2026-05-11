import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { analyticsApi } from '../lib/api'
import { ServiceCard } from './ServiceCard'
import type { ServiceMetrics } from '../types'

export function ServiceHealthGrid() {
  const navigate = useNavigate()
  const { data, isLoading, isError, refetch } = useQuery<ServiceMetrics[]>({
    queryKey: ['analytics', 'services'],
    queryFn: () => analyticsApi.services().then((r) => r.data),
    staleTime: 25_000,
    refetchInterval: 30_000,
  })

  if (isLoading) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-3">
        {[...Array(6)].map((_, i) => (
          <div key={i} className="bg-slate-800 rounded-lg h-32 animate-pulse" />
        ))}
      </div>
    )
  }

  if (isError) {
    return (
      <div className="bg-slate-800 rounded-lg p-6 text-center">
        <p className="text-slate-400 mb-3">Failed to load service health</p>
        <button
          onClick={() => refetch()}
          className="px-4 py-2 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded text-sm transition-colors"
        >
          Retry
        </button>
      </div>
    )
  }

  const services = data ?? []

  if (services.length === 0) {
    return (
      <div className="bg-slate-800 rounded-lg p-6 text-center text-slate-500 text-sm">
        No services reporting yet. Send some logs to get started.
      </div>
    )
  }

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-3">
      {services.map((s) => (
        <ServiceCard
          key={s.service_name}
          service={s}
          onClick={() => navigate(`/services/${s.service_name}`)}
        />
      ))}
    </div>
  )
}
