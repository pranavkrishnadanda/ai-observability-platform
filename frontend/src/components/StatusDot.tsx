type DotStatus = 'healthy' | 'degraded' | 'critical' | 'connected' | 'disconnected' | 'connecting'

const DOT_STYLES: Record<DotStatus, string> = {
  healthy: 'bg-emerald-400',
  connected: 'bg-emerald-400',
  degraded: 'bg-yellow-400',
  connecting: 'bg-yellow-400 animate-pulse',
  critical: 'bg-red-500',
  disconnected: 'bg-red-500',
}

const PULSE_STATUSES: DotStatus[] = ['healthy', 'connected']

export function StatusDot({ status }: { status: DotStatus }) {
  const pulse = PULSE_STATUSES.includes(status)
  return (
    <span className="relative inline-flex h-2 w-2">
      {pulse && (
        <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${DOT_STYLES[status]}`} />
      )}
      <span className={`relative inline-flex rounded-full h-2 w-2 ${DOT_STYLES[status]}`} />
    </span>
  )
}
