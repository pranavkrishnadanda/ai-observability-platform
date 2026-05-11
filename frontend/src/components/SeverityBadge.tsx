import type { Log } from '../types'

const SEVERITY_STYLES: Record<Log['severity'], string> = {
  DEBUG: 'bg-slate-700 text-slate-300',
  INFO: 'bg-blue-900/60 text-blue-300',
  WARNING: 'bg-yellow-900/60 text-yellow-300',
  ERROR: 'bg-red-900/60 text-red-300',
  CRITICAL: 'bg-red-800 text-red-100 font-bold',
}

export function SeverityBadge({ severity }: { severity: Log['severity'] }) {
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-mono ${SEVERITY_STYLES[severity]}`}>
      {severity}
    </span>
  )
}
