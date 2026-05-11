import type { ReactNode } from 'react'
import { Navigate } from 'react-router-dom'
import { apiKeys } from '../lib/api'

export function ProtectedRoute({ children }: { children: ReactNode }) {
  if (!apiKeys.exists()) {
    return <Navigate to="/login" replace />
  }
  return <>{children}</>
}
