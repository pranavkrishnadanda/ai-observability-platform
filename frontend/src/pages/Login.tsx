import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAppStore } from '../store'

export default function Login() {
  const navigate = useNavigate()
  const { apiKey, setApiKey } = useAppStore()
  const [input, setInput] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [registering, setRegistering] = useState(false)
  const [orgName, setOrgName] = useState('')
  const [showRegister, setShowRegister] = useState(false)

  useEffect(() => {
    if (apiKey) navigate('/dashboard', { replace: true })
  }, [apiKey, navigate])

  async function handleConnect(e: React.FormEvent) {
    e.preventDefault()
    if (!input.trim()) {
      setError('API key is required')
      return
    }
    setLoading(true)
    setError('')
    try {
      const res = await fetch('/api/v1/tenant/settings', {
        headers: { 'X-API-Key': input.trim() },
      })
      if (res.status === 401 || res.status === 422) {
        setError('Invalid API key')
        return
      }
      setApiKey(input.trim())
      navigate('/dashboard', { replace: true })
    } catch {
      // Network error — store key anyway, dashboard will show connection state
      setApiKey(input.trim())
      navigate('/dashboard', { replace: true })
    } finally {
      setLoading(false)
    }
  }

  async function handleRegister(e: React.FormEvent) {
    e.preventDefault()
    const name = orgName.trim() || `org-${Date.now()}`
    setRegistering(true)
    setError('')
    try {
      const res = await fetch('/api/v1/auth/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      })
      if (!res.ok) {
        const body = await res.json() as { detail?: string }
        setError(body.detail ?? 'Registration failed')
        return
      }
      const data = await res.json() as { api_key: string }
      setApiKey(data.api_key)
      navigate('/dashboard', { replace: true })
    } catch {
      setError('Could not reach the backend. Is it running?')
    } finally {
      setRegistering(false)
    }
  }

  return (
    <div className="min-h-screen bg-slate-900 flex items-center justify-center px-4">
      <div className="w-full max-w-sm">
        <div className="text-center mb-8">
          <h1 className="text-2xl font-bold text-cyan-400 font-mono mb-2">
            AI Observability
          </h1>
          <p className="text-slate-400 text-sm">
            {showRegister ? 'Create a new account' : 'Sign in with your API key'}
          </p>
        </div>

        <div className="bg-slate-800 rounded-xl border border-slate-700 shadow-xl overflow-hidden">
          {/* Tab switcher */}
          <div className="flex border-b border-slate-700">
            <button
              onClick={() => { setShowRegister(false); setError('') }}
              className={`flex-1 py-2.5 text-xs font-mono transition-colors ${
                !showRegister
                  ? 'bg-slate-700 text-cyan-400'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              Sign In
            </button>
            <button
              onClick={() => { setShowRegister(true); setError('') }}
              className={`flex-1 py-2.5 text-xs font-mono transition-colors ${
                showRegister
                  ? 'bg-slate-700 text-cyan-400'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              Register
            </button>
          </div>

          <div className="p-6">
            {!showRegister ? (
              <form onSubmit={handleConnect}>
                <div className="mb-4">
                  <label className="block text-xs text-slate-400 mb-2 font-mono">
                    API Key
                  </label>
                  <input
                    type="password"
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    placeholder="aiobs_..."
                    autoFocus
                    className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2.5 text-sm font-mono text-slate-200 placeholder-slate-500 focus:outline-none focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500 transition-colors"
                  />
                </div>
                {error && (
                  <p className="text-red-400 text-xs mb-4 font-mono">{error}</p>
                )}
                <button
                  type="submit"
                  disabled={loading}
                  className="w-full py-2.5 bg-cyan-600 hover:bg-cyan-500 disabled:opacity-50 text-white font-mono text-sm rounded transition-colors"
                >
                  {loading ? 'Connecting…' : 'Connect'}
                </button>
              </form>
            ) : (
              <form onSubmit={handleRegister}>
                <div className="mb-4">
                  <label className="block text-xs text-slate-400 mb-2 font-mono">
                    Organization Name <span className="text-slate-600">(optional)</span>
                  </label>
                  <input
                    type="text"
                    value={orgName}
                    onChange={(e) => setOrgName(e.target.value)}
                    placeholder="my-company"
                    autoFocus
                    className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2.5 text-sm font-mono text-slate-200 placeholder-slate-500 focus:outline-none focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500 transition-colors"
                  />
                  <p className="text-xs text-slate-600 mt-1.5 font-mono">
                    Leave blank for a random name
                  </p>
                </div>
                {error && (
                  <p className="text-red-400 text-xs mb-4 font-mono">{error}</p>
                )}
                <button
                  type="submit"
                  disabled={registering}
                  className="w-full py-2.5 bg-emerald-700 hover:bg-emerald-600 disabled:opacity-50 text-white font-mono text-sm rounded transition-colors"
                >
                  {registering ? 'Creating account…' : 'Create Account & Connect'}
                </button>
              </form>
            )}
          </div>
        </div>

        <p className="text-center text-xs text-slate-600 mt-4 font-mono">
          Backend: localhost:8000
        </p>
      </div>
    </div>
  )
}
