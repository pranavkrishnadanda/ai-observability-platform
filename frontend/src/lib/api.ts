import axios from 'axios'
import type { AnalyticsOverview, Anomaly, Log, ServiceMetrics, TimelinePoint, HealthStatus } from '../types'

const API_KEY_STORAGE = 'obs_api_key'

export const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL,
})

api.interceptors.request.use((config) => {
  const key = localStorage.getItem(API_KEY_STORAGE)
  if (key) config.headers['X-API-Key'] = key
  return config
})

api.interceptors.response.use(
  (response) => response,
  (error: unknown) => {
    if (
      axios.isAxiosError(error) &&
      error.response?.status === 401
    ) {
      localStorage.removeItem(API_KEY_STORAGE)
      window.location.href = '/login'
    }
    return Promise.reject(error)
  }
)

export const apiKeys = {
  get: () => localStorage.getItem(API_KEY_STORAGE),
  set: (key: string) => localStorage.setItem(API_KEY_STORAGE, key),
  clear: () => localStorage.removeItem(API_KEY_STORAGE),
  exists: () => !!localStorage.getItem(API_KEY_STORAGE),
}

interface LogSearchParams {
  service?: string
  severity?: string
  from_time?: string
  to_time?: string
  search_text?: string
  limit?: number
  offset?: number
}

interface AnomalyListParams {
  service?: string
  status?: string
  severity_min?: number
  limit?: number
  offset?: number
}

interface AlertListParams {
  service?: string
  status?: string
}

export const logsApi = {
  search: (params: LogSearchParams) =>
    api.get<Log[]>('/api/v1/logs', { params }),
  getById: (id: string) =>
    api.get<Log>(`/api/v1/logs/${id}`),
}

export const anomaliesApi = {
  list: (params: AnomalyListParams) =>
    api.get<Anomaly[]>('/api/v1/anomalies', { params }),
  getById: (id: string) =>
    api.get<Anomaly>(`/api/v1/anomalies/${id}`),
  acknowledge: (id: string) =>
    api.patch<Anomaly>(`/api/v1/anomalies/${id}/acknowledge`),
  resolve: (id: string) =>
    api.patch<Anomaly>(`/api/v1/anomalies/${id}/resolve`),
}

export const alertsApi = {
  list: (params: AlertListParams) =>
    api.get('/api/v1/alerts', { params }),
  stats: () => api.get('/api/v1/alerts/stats'),
}

export const analyticsApi = {
  overview: () =>
    api.get<AnalyticsOverview>('/api/v1/analytics/overview'),
  services: () =>
    api.get<ServiceMetrics[]>('/api/v1/analytics/services'),
  serviceTimeline: (name: string) =>
    api.get<TimelinePoint[]>(
      `/api/v1/analytics/services/${name}/timeline`
    ),
}

export const healthApi = {
  check: () => api.get<HealthStatus>('/health'),
}

export const authApi = {
  register: (name: string) =>
    api.post<{ api_key: string }>('/api/v1/auth/register', { name }),
  rotateKey: () =>
    api.post<{ api_key: string }>('/api/v1/auth/rotate-key'),
}
