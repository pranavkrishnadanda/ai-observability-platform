import { ServiceHealthGrid } from '../components/ServiceHealthGrid'

export default function Services() {
  return (
    <div className="flex flex-col gap-4">
      <h1 className="text-lg font-mono font-bold text-slate-200">Services</h1>
      <ServiceHealthGrid />
    </div>
  )
}
