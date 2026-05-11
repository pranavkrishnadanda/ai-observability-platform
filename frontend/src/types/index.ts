export interface Tenant {
  id: string
  name: string
  plan_tier: string
  rate_limit_per_minute: number
  created_at: string
}

export interface Log {
  id: string
  service_name: string
  severity: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR' | 'CRITICAL'
  message: string
  metadata: Record<string, unknown>
  trace_id: string | null
  span_id: string | null
  environment: string
  created_at: string
  ingested_at: string
}

export interface Anomaly {
  id: string
  tenant_id: string
  service_name: string
  anomaly_type: string
  severity_score: number
  detected_at: string
  window_start: string
  window_end: string
  baseline_value: number | null
  observed_value: number | null
  deviation_pct: number | null
  claude_analysis: string | null
  status: 'active' | 'resolved' | 'acknowledged'
  resolved_at: string | null
}

export interface Alert {
  id: string
  tenant_id: string
  anomaly_id: string
  alert_type: string
  severity: string
  title: string
  description: string
  delivery_status: string
  created_at: string
}

export interface ServiceMetrics {
  service_name: string
  health_status: 'healthy' | 'degraded' | 'critical'
  log_volume_1h: number
  log_volume_24h: number
  error_rate_1h: number
  error_rate_24h: number
  anomaly_count_7d: number
  last_seen: string
}

export interface AnalyticsOverview {
  total_logs_today: number
  total_logs_yesterday: number
  error_rate_today: number
  error_rate_yesterday: number
  active_anomalies: number
  alerts_sent_today: number
  system_health_score: number
}

export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  per_page: number
}

export interface TimelinePoint {
  hour: string
  total_logs: number
  error_count: number
  error_rate: number
}

export interface HealthStatus {
  status: string
  postgres: string
  redis: string
  kafka: string
}
