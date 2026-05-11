import { create } from 'zustand'

interface AppStore {
  apiKey: string | null
  setApiKey: (key: string) => void
  clearApiKey: () => void
}

export const useAppStore = create<AppStore>((set) => ({
  apiKey: localStorage.getItem('obs_api_key'),
  setApiKey: (key) => {
    localStorage.setItem('obs_api_key', key)
    set({ apiKey: key })
  },
  clearApiKey: () => {
    localStorage.removeItem('obs_api_key')
    set({ apiKey: null })
  },
}))
