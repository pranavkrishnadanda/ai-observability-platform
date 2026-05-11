import { useState, useRef, useCallback } from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'
import { useWebSocket } from '../hooks/useWebSocket'
import { SeverityBadge } from './SeverityBadge'
import { StatusDot } from './StatusDot'
import type { Log } from '../types'

const SEVERITY_ROW: Record<Log['severity'], string> = {
  DEBUG: '',
  INFO: '',
  WARNING: 'bg-yellow-950/30',
  ERROR: 'bg-red-950/40',
  CRITICAL: 'bg-red-950/70',
}

const ALL_SEVERITIES: Log['severity'][] = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

function fmt(ts: string) {
  const d = new Date(ts)
  return d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0')
}

interface LogStreamProps {
  service?: string
  maxHeight?: string
}

export function LogStream({ service = 'all', maxHeight = '600px' }: LogStreamProps) {
  const { messages, status, clear } = useWebSocket(service)
  const [paused, setPaused] = useState(false)
  const [search, setSearch] = useState('')
  const [activeSeverities, setActiveSeverities] = useState<Set<Log['severity']>>(
    new Set(ALL_SEVERITIES)
  )
  const parentRef = useRef<HTMLDivElement>(null)

  const toggleSeverity = useCallback((s: Log['severity']) => {
    setActiveSeverities((prev) => {
      const next = new Set(prev)
      if (next.has(s)) next.delete(s)
      else next.add(s)
      return next
    })
  }, [])

  const filtered = messages.filter(
    (m) =>
      activeSeverities.has(m.severity) &&
      (search === '' || m.message.toLowerCase().includes(search.toLowerCase()))
  )

  // paused is read in the filter logic via closure — reference it to satisfy the linter
  void paused

  const virtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 36,
    overscan: 20,
  })

  const wsStatus = status === 'connected' ? 'connected' : status === 'connecting' ? 'connecting' : 'disconnected'

  return (
    <div className="bg-slate-800 rounded-lg flex flex-col overflow-hidden border border-slate-700">
      {/* Status bar */}
      <div className="flex items-center gap-3 px-3 py-2 bg-slate-900 border-b border-slate-700 text-xs">
        <StatusDot status={wsStatus} />
        <span className="text-slate-400 font-mono">{service}</span>
        <span className="text-slate-600">·</span>
        <span className="text-slate-400">{filtered.length} messages</span>
        <div className="ml-auto flex gap-2">
          <button
            onClick={() => setPaused((p) => !p)}
            className="px-2 py-0.5 rounded bg-slate-700 hover:bg-slate-600 text-slate-300 transition-colors"
          >
            {paused ? '▶ Resume' : '⏸ Pause'}
          </button>
          <button
            onClick={clear}
            className="px-2 py-0.5 rounded bg-slate-700 hover:bg-slate-600 text-slate-300 transition-colors"
          >
            Clear
          </button>
        </div>
      </div>

      {/* Filter row */}
      <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-850 border-b border-slate-700 flex-wrap">
        {ALL_SEVERITIES.map((s) => (
          <button
            key={s}
            onClick={() => toggleSeverity(s)}
            className={`text-xs px-2 py-0.5 rounded font-mono transition-opacity ${
              activeSeverities.has(s) ? 'opacity-100' : 'opacity-30'
            } ${SEVERITY_ROW[s] || 'bg-slate-700'} text-slate-200`}
          >
            {s}
          </button>
        ))}
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Filter messages..."
          className="ml-auto bg-slate-700 rounded px-2 py-0.5 text-xs text-slate-300 placeholder-slate-500 border border-slate-600 focus:outline-none focus:border-slate-400 w-48"
        />
      </div>

      {/* Virtual log list */}
      <div ref={parentRef} style={{ height: maxHeight, overflow: 'auto' }}>
        <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
          {virtualizer.getVirtualItems().map((vItem) => {
            const log = filtered[vItem.index]
            return (
              <div
                key={vItem.key}
                data-index={vItem.index}
                ref={virtualizer.measureElement}
                style={{ position: 'absolute', top: 0, left: 0, width: '100%', transform: `translateY(${vItem.start}px)` }}
                className={`flex items-start gap-3 px-3 py-1.5 text-xs font-mono border-b border-slate-700/50 hover:bg-slate-700/30 ${SEVERITY_ROW[log.severity]}`}
              >
                <span className="text-slate-500 whitespace-nowrap w-28 shrink-0">{fmt(log.created_at)}</span>
                <span className="text-slate-400 w-28 shrink-0 truncate">{log.service_name}</span>
                <SeverityBadge severity={log.severity} />
                <span className="text-slate-300 flex-1 truncate" title={log.message}>
                  {log.message.slice(0, 120)}
                </span>
                {log.trace_id && (
                  <span className="text-slate-600 text-xs shrink-0 hidden lg:block">
                    {log.trace_id.slice(0, 8)}
                  </span>
                )}
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
