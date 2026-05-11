import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AnomalyCard } from '../components/AnomalyCard'
import { anomaliesApi } from '../lib/api'
import type { Anomaly } from '../types'

type StatusFilter = 'all' | 'active' | 'acknowledged' | 'resolved'

export default function Anomalies() {
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all')
  const [serviceFilter, setServiceFilter] = useState('')
  const [severityMin, setSeverityMin] = useState(0)
  const [page, setPage] = useState(0)
  const PAGE_SIZE = 20

  const { data, refetch, isLoading } = useQuery<Anomaly[]>({
    queryKey: ['anomalies', 'all', statusFilter, serviceFilter, severityMin, page],
    queryFn: () =>
      anomaliesApi
        .list({
          status: statusFilter === 'all' ? undefined : statusFilter,
          service: serviceFilter || undefined,
          severity_min: severityMin > 0 ? severityMin / 100 : undefined,
          limit: PAGE_SIZE,
          offset: page * PAGE_SIZE,
        })
        .then((r) => r.data),
    staleTime: 8_000,
    refetchInterval: 15_000,
  })

  const items = data ?? []

  return (
    <div className="flex flex-col gap-4 max-w-4xl">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-mono font-bold text-slate-200">Anomalies</h1>
        <button
          onClick={() => refetch()}
          className="text-xs text-slate-400 hover:text-slate-200 transition-colors font-mono"
        >
          ↻ Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="bg-slate-800 rounded-lg p-3 flex flex-wrap gap-3 items-center">
        <div className="flex gap-1">
          {(['all', 'active', 'acknowledged', 'resolved'] as StatusFilter[]).map((s) => (
            <button
              key={s}
              onClick={() => { setStatusFilter(s); setPage(0) }}
              className={`px-3 py-1 rounded text-xs font-mono transition-colors capitalize ${
                statusFilter === s
                  ? 'bg-cyan-700 text-cyan-100'
                  : 'bg-slate-700 text-slate-400 hover:text-slate-200'
              }`}
            >
              {s}
            </button>
          ))}
        </div>
        <input
          type="text"
          value={serviceFilter}
          onChange={(e) => { setServiceFilter(e.target.value); setPage(0) }}
          placeholder="Filter by service..."
          className="bg-slate-700 border border-slate-600 rounded px-2 py-1 text-xs font-mono text-slate-300 placeholder-slate-500 focus:outline-none focus:border-slate-400 w-48"
        />
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-500 font-mono">Min severity:</span>
          <input
            type="range"
            min={0}
            max={100}
            value={severityMin}
            onChange={(e) => { setSeverityMin(Number(e.target.value)); setPage(0) }}
            className="w-20 accent-cyan-500"
          />
          <span className="text-xs text-slate-400 font-mono w-8">{severityMin}%</span>
        </div>
      </div>

      {/* List */}
      {isLoading ? (
        <div className="flex flex-col gap-2">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="bg-slate-800 rounded-lg h-28 animate-pulse" />
          ))}
        </div>
      ) : items.length === 0 ? (
        <div className="bg-slate-800 rounded-lg p-8 text-center text-slate-500 font-mono text-sm">
          No anomalies found
        </div>
      ) : (
        <div className="flex flex-col gap-2">
          {items.map((a) => (
            <AnomalyCard
              key={a.id}
              anomaly={a}
              onUpdate={() => refetch()}
            />
          ))}
        </div>
      )}

      {/* Pagination */}
      <div className="flex justify-between items-center">
        <button
          disabled={page === 0}
          onClick={() => setPage((p) => p - 1)}
          className="px-3 py-1.5 text-xs font-mono bg-slate-700 hover:bg-slate-600 disabled:opacity-30 rounded transition-colors text-slate-300"
        >
          ← Previous
        </button>
        <span className="text-xs text-slate-500 font-mono">Page {page + 1}</span>
        <button
          disabled={items.length < PAGE_SIZE}
          onClick={() => setPage((p) => p + 1)}
          className="px-3 py-1.5 text-xs font-mono bg-slate-700 hover:bg-slate-600 disabled:opacity-30 rounded transition-colors text-slate-300"
        >
          Next →
        </button>
      </div>
    </div>
  )
}
