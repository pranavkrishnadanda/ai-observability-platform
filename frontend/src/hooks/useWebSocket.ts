import { useEffect, useRef, useState, useCallback } from 'react'
import type { Log } from '../types'

const WS_MAX_MESSAGES = 500

type WsStatus = 'connecting' | 'connected' | 'disconnected'

interface UseWebSocketReturn {
  messages: Log[]
  status: WsStatus
  clear: () => void
}

export function useWebSocket(service: string = 'all'): UseWebSocketReturn {
  const [messages, setMessages] = useState<Log[]>([])
  const [status, setStatus] = useState<WsStatus>('disconnected')
  const wsRef = useRef<WebSocket | null>(null)
  const retryRef = useRef<number>(0)
  const retryTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined)
  const mountedRef = useRef(true)

  const connect = useCallback(() => {
    const apiKey = localStorage.getItem('obs_api_key')
    if (!apiKey || !mountedRef.current) return

    const wsBase =
      import.meta.env.VITE_WS_URL ?? 'ws://localhost:8000'
    setStatus('connecting')
    const ws = new WebSocket(
      `${wsBase}/ws/logs/${service}?token=${apiKey}`
    )

    ws.onopen = () => {
      if (!mountedRef.current) { ws.close(); return }
      setStatus('connected')
      retryRef.current = 0
    }

    ws.onmessage = (event: MessageEvent) => {
      try {
        const log = JSON.parse(event.data as string) as Log
        setMessages((prev) => [log, ...prev].slice(0, WS_MAX_MESSAGES))
      } catch {
        // ignore malformed frames
      }
    }

    ws.onclose = () => {
      if (!mountedRef.current) return
      setStatus('disconnected')
      const delay = Math.min(1000 * Math.pow(2, retryRef.current), 30000)
      retryRef.current += 1
      retryTimerRef.current = setTimeout(connect, delay)
    }

    ws.onerror = () => { ws.close() }
    wsRef.current = ws
  }, [service])

  useEffect(() => {
    mountedRef.current = true
    connect()
    return () => {
      mountedRef.current = false
      clearTimeout(retryTimerRef.current)
      wsRef.current?.close()
    }
  }, [connect])

  const clear = useCallback(() => setMessages([]), [])
  return { messages, status, clear }
}
