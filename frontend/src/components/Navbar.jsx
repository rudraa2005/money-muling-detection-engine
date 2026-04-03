import { Link, useLocation } from 'react-router-dom'

export default function Navbar() {
  const location = useLocation()
  const isActive = (path) => location.pathname === path

  return (
    <nav className="fixed top-4 left-1/2 z-50 w-[min(96%,84rem)] -translate-x-1/2 rounded-[28px] border border-white/10 bg-[rgba(12,13,15,0.82)] backdrop-blur-xl shadow-[0_24px_70px_rgba(0,0,0,0.32)]">
      <div className="flex flex-col gap-3 px-4 py-3 md:flex-row md:items-center md:justify-between md:gap-6 md:px-6">
        <Link to="/" className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-2xl border border-white/10 bg-[linear-gradient(145deg,rgba(116,130,150,0.24),rgba(243,239,230,0.1))]">
            <span className="material-symbols-outlined text-[18px] text-primary">hub</span>
          </div>
          <div className="leading-tight">
            <span className="block font-display text-base tracking-tight text-white">GraphLens</span>
            <span className="block text-[10px] uppercase tracking-[0.22em] text-text-muted font-body">Money Muling Detection</span>
          </div>
        </Link>

        <div className="hide-scrollbar overflow-x-auto">
          <div className="flex min-w-max items-center gap-1 rounded-full border border-white/8 bg-white/[0.03] p-1">
            {[
              { to: '/', label: 'Home' },
              { to: '/network-graph', label: 'Network Graph' },
              { to: '/fraud-rings', label: 'Fraud Rings' },
              { to: '/reports', label: 'Reports' },
              { to: '/analytics', label: 'Analytics' },
              { to: '/history', label: 'History' },
            ].map(({ to, label }) => (
              <Link
                key={to}
                to={to}
                className={`rounded-full px-4 py-2 text-xs font-semibold transition-all duration-200 ${
                  isActive(to)
                    ? 'border border-white/10 bg-white/[0.08] text-primary'
                    : 'text-text-muted hover:bg-white/[0.05] hover:text-white'
                }`}
              >
                {label}
              </Link>
            ))}
          </div>
        </div>
      </div>
    </nav>
  )
}
