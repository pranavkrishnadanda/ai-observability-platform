import type { ReactNode } from 'react'
import { Link, useNavigate, useLocation } from 'react-router-dom'
import { HealthIndicator } from './HealthIndicator'
import { useAppStore } from '../store'

const NAV_LINKS = [
  { to: '/dashboard', label: 'Dashboard' },
  { to: '/services', label: 'Services' },
  { to: '/anomalies', label: 'Anomalies' },
]

export function Layout({ children }: { children: ReactNode }) {
  const navigate = useNavigate()
  const location = useLocation()
  const { apiKey, clearApiKey } = useAppStore()

  function logout() {
    clearApiKey()
    navigate('/login')
  }

  return (
    <div className="min-h-screen bg-slate-900 flex flex-col">
      <nav className="bg-slate-800 border-b border-slate-700 px-4 h-12 flex items-center gap-4 shrink-0">
        <Link to="/dashboard" className="font-mono text-sm font-bold text-cyan-400 mr-2 whitespace-nowrap">
          AI Observability
        </Link>
        <div className="flex items-center gap-1">
          {NAV_LINKS.map(({ to, label }) => (
            <Link
              key={to}
              to={to}
              className={`px-3 py-1.5 rounded text-xs font-mono transition-colors ${
                location.pathname.startsWith(to)
                  ? 'bg-slate-700 text-cyan-400'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700'
              }`}
            >
              {label}
            </Link>
          ))}
        </div>
        <div className="ml-auto flex items-center gap-3">
          <HealthIndicator />
          {apiKey && (
            <span className="text-xs text-slate-500 font-mono hidden sm:block">
              …{apiKey.slice(-8)}
            </span>
          )}
          <button
            onClick={logout}
            className="text-xs text-slate-400 hover:text-slate-200 transition-colors px-2 py-1 rounded hover:bg-slate-700"
          >
            Logout
          </button>
        </div>
      </nav>
      <main className="flex-1 p-4">
        {children}
      </main>
    </div>
  )
}
