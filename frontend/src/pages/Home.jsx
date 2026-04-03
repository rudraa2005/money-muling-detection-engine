import { useState, useRef, useEffect } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import Lenis from 'lenis'
import Navbar from '../components/Navbar'
import Footer from '../components/Footer'
import { useAnalysis } from '../context/AnalysisContext'
import { buildStrictExportPayload } from '../utils/jsonExport'

/* ─── Animated counter ─── */
function AnimatedCounter({ target, suffix = '', duration = 2000 }) {
  const [count, setCount] = useState(0)
  const ref = useRef(null)
  const hasAnimated = useRef(false)

  useEffect(() => {
    const el = ref.current
    if (!el) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && !hasAnimated.current) {
          hasAnimated.current = true
          const start = performance.now()
          const step = (now) => {
            const elapsed = now - start
            const progress = Math.min(elapsed / duration, 1)
            const eased = 1 - Math.pow(1 - progress, 3)
            setCount(Math.floor(eased * target))
            if (progress < 1) requestAnimationFrame(step)
          }
          requestAnimationFrame(step)
        }
      },
      { threshold: 0.3 }
    )
    observer.observe(el)
    return () => observer.disconnect()
  }, [target, duration])

  return <span ref={ref}>{count}{suffix}</span>
}

/* ─── Scroll reveal ─── */
function useScrollReveal() {
  const ref = useRef(null)
  useEffect(() => {
    const el = ref.current
    if (!el) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) { el.classList.add('revealed'); observer.unobserve(el) }
      },
      { threshold: 0.15 }
    )
    observer.observe(el)
    return () => observer.disconnect()
  }, [])
  return ref
}

export default function Home() {
  const { uploadAndAnalyze, isUploading, error, analysis, health } = useAnalysis()
  const navigate = useNavigate()
  const heroRef = useScrollReveal()
  const uploadRef = useScrollReveal()
  const statsRef = useScrollReveal()
  const modulesRef = useScrollReveal()
  const [selectedFileName, setSelectedFileName] = useState('')

  /* Lenis smooth scroll */
  useEffect(() => {
    const lenis = new Lenis({
      duration: 1.4,
      easing: (t) => Math.min(1, 1.001 - Math.pow(2, -10 * t)),
      smooth: true,
    })
    function raf(time) { lenis.raf(time); requestAnimationFrame(raf) }
    requestAnimationFrame(raf)
    return () => lenis.destroy()
  }, [])

  return (
    <div className="bg-background-dark text-[color:var(--foreground)] font-display min-h-screen flex flex-col">
      <Navbar />

      <main className="flex-1 mt-20 md:mt-24">
        {/* ═══════ Hero ═══════ */}
        <section ref={heroRef} className="scroll-reveal relative flex min-h-[88vh] items-center justify-center overflow-hidden px-6 py-10">
          <div className="absolute inset-0 z-0">
            <img src="/hero_network.jpg" alt="Financial Network" className="h-full w-full object-cover opacity-45" />
            <div className="absolute inset-0 bg-gradient-to-b from-background-dark via-[rgba(12,13,16,0.45)] to-background-dark"></div>
            <div className="absolute inset-0 bg-[radial-gradient(circle_at_center,transparent_0%,rgba(0,0,0,0.48)_100%)]"></div>
          </div>

          <div className="relative z-10 mx-auto max-w-5xl text-center">
            <div className="mx-auto max-w-4xl rounded-[32px] border border-white/10 bg-[rgba(14,15,17,0.42)] px-6 py-10 shadow-[0_30px_80px_rgba(0,0,0,0.28)] backdrop-blur-sm md:px-10 md:py-12">
              <div className="animate-fade-in-up mb-8 inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-4 py-1.5 backdrop-blur-md">
                <span className="size-1.5 rounded-full bg-accent-blue animate-pulse"></span>
                <span className="text-[11px] font-medium tracking-[0.22em] text-[#d9d4ca] font-body uppercase">Detection Console Ready</span>
              </div>

              <h1 className="animate-fade-in-up delay-200 mb-8 text-6xl font-medium leading-[0.95] tracking-tight text-white md:text-[6.75rem]">
                Financial Network
                <br />
                Intelligence
              </h1>

              <p className="animate-fade-in-up delay-400 mx-auto mb-12 max-w-2xl text-lg leading-relaxed text-neutral-300 font-body md:text-xl">
                Detect suspicious fund movement with graph-native analysis, behavioral scoring, and a calmer investigation surface built for long review sessions.
              </p>

              <div className="animate-fade-in-up delay-500 flex flex-col items-center justify-center gap-4 sm:flex-row sm:gap-5">
                <button
                  type="button"
                  onClick={() => document.getElementById('upload-csv')?.scrollIntoView({ behavior: 'smooth', block: 'start' })}
                  className="btn-primary rounded-full px-8 py-3.5 font-semibold"
                >
                  Get Started
                </button>
                <Link to="/network-graph" className="btn-outline rounded-full px-8 py-3.5 font-medium">
                  View Live Graph
                </Link>
              </div>

              <div className="animate-fade-in-up delay-600 mt-10 flex flex-wrap items-center justify-center gap-3">
                {['20 detection patterns', 'Graph + ML scoring', 'Historical run archive'].map((label) => (
                  <span key={label} className="rounded-full border border-white/10 bg-white/[0.03] px-3.5 py-1.5 text-[11px] uppercase tracking-[0.18em] text-[#bcb7ad] font-body">
                    {label}
                  </span>
                ))}
              </div>
            </div>
          </div>
        </section>

        {/* ═══════ Upload ═══════ */}
        <section id="upload-csv" ref={uploadRef} className="scroll-reveal relative px-6 py-28">
          <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/10 to-transparent"></div>

          <div className="absolute top-0 left-0 h-[420px] w-40 bg-accent-blue/8 blur-[110px]"></div>
          <div className="absolute bottom-0 right-0 h-[420px] w-40 bg-accent-purple/8 blur-[110px]"></div>

          <div className="max-w-3xl mx-auto text-center relative z-10">
            <h2 className="text-5xl font-display text-white mb-4 tracking-tight">Upload & Analyze</h2>
            <p className="text-base text-neutral-400 mb-4 font-body">Drop your transaction data to begin fraud detection analysis.</p>
            {health && (
              <p className="text-xs text-neutral-500 mb-6 font-body">
                Backend status: <span className={health.status === 'healthy' ? 'text-emerald-400' : 'text-amber-300'}>{health.status}</span>
              </p>
            )}

            <div className="card-glass relative overflow-hidden rounded-[32px] border border-white/10 bg-card-dark/90 p-10 shadow-[0_32px_90px_rgba(0,0,0,0.24)] backdrop-blur-xl group md:p-12">
              <div className="absolute inset-0 bg-gradient-to-br from-white/[0.05] via-transparent to-accent-blue/10 opacity-0 transition-opacity duration-700 group-hover:opacity-100"></div>

              <div className="flex flex-col items-center gap-6 relative z-10">
                <div className="flex h-16 w-16 items-center justify-center rounded-2xl border border-white/10 bg-white/[0.04] transition-transform duration-500 group-hover:scale-105">
                  <span className="material-symbols-outlined text-white text-3xl">upload_file</span>
                </div>
                <div>
                  <p className="text-lg font-medium text-white mb-2 font-display">Upload Transaction Data</p>
                  <p className="text-sm text-neutral-500 font-body">
                    {selectedFileName || 'Drop your CSV file here or click to browse'}
                  </p>
                </div>
                <label className="btn-primary inline-flex cursor-pointer items-center gap-3 rounded-full px-8 py-3 text-sm font-semibold">
                  <span>{isUploading ? 'Analyzing…' : 'Select CSV File'}</span>
                  <input
                    type="file"
                    accept=".csv"
                    className="hidden"
                    onChange={async (e) => {
                      const file = e.target.files?.[0]
                      if (!file) return
                      setSelectedFileName(file.name)
                      try {
                        const result = await uploadAndAnalyze(file)
                        if (result?.graph_data) {
                          navigate('/network-graph')
                        } else {
                          navigate('/reports')
                        }
                      } catch {
                        // error is surfaced below
                      } finally {
                        e.target.value = ''
                      }
                    }}
                  />
                </label>
                {error && (
                  <p className="text-xs text-[#d59b86] font-body">
                    {error.message || 'Failed to analyze file. Please try again.'}
                  </p>
                )}
                {analysis && !error && !isUploading && (
                  <div className="flex flex-col items-center gap-4">
                    <p className="text-xs text-neutral-400 font-body">
                      Last run: {analysis.summary?.suspicious_accounts_flagged ?? 0} suspicious accounts,{' '}
                      {analysis.summary?.fraud_rings_detected ?? 0} rings detected.
                    </p>
                    <button
                      onClick={() => {
                        const finalData = buildStrictExportPayload(analysis)
                        const blob = new Blob([JSON.stringify(finalData, null, 2)], { type: 'application/json' })
                        const url = URL.createObjectURL(blob)
                        const a = document.createElement('a')
                        a.href = url
                        a.download = `fraud_detection_${new Date().toISOString().split('T')[0]}.json`
                        a.click()
                        URL.revokeObjectURL(url)
                      }}
                      className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-6 py-2 text-xs font-semibold text-white transition-colors hover:bg-white/[0.08]"
                    >
                      <span className="material-symbols-outlined text-sm">download</span>
                      Download Results (JSON)
                    </button>
                  </div>
                )}
              </div>
            </div>
          </div>
        </section>

        {/* ═══════ Stats ═══════ */}
        <section ref={statsRef} className="scroll-reveal relative px-6 py-20">
          <div className="max-w-4xl mx-auto grid grid-cols-1 md:grid-cols-3 gap-8 relative z-10">
            {[
              { target: 2.4, suffix: 'B+', label: 'Nodes Analyzed', text: null },
              { target: 99.9, suffix: '%', label: 'Uptime', text: null },
              { target: 0, suffix: '', label: 'Real-time Detection', text: 'Real-time' },
            ].map((stat, i) => (
              <div key={i} className="relative overflow-hidden rounded-[28px] border border-white/8 bg-card-dark/80 p-8 text-center backdrop-blur-sm transition-all duration-300 group hover:border-white/12">
                <div className="absolute inset-0 bg-gradient-to-b from-white/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity"></div>
                <p className="text-4xl font-normal text-white mb-2 font-display relative z-10">
                  {stat.text || <AnimatedCounter target={stat.target * 10} suffix={stat.suffix} />}
                </p>
                <p className="text-sm text-neutral-500 font-medium font-body relative z-10">{stat.label}</p>
              </div>
            ))}
          </div>
        </section>

        {/* ═══════ Modules ═══════ */}
        <section ref={modulesRef} className="scroll-reveal relative px-6 py-32 pb-40">
          <div className="absolute bottom-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/10 to-transparent"></div>

          <div className="max-w-6xl mx-auto relative z-10">
            <h2 className="text-4xl font-display text-white mb-3 tracking-tight text-center">Core Intelligence Modules</h2>
            <p className="text-base text-neutral-400 text-center mb-16 font-body">Comprehensive tools for financial network analysis</p>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {[
                { icon: 'grid_view', title: 'Global Dashboard', desc: 'Unified view of all monitored entities with real-time risk scoring.', to: '/network-graph', color: 'text-accent-blue' },
                { icon: 'hub', title: 'Fraud Ring Detection', desc: 'Automatically identify circular transaction patterns and clusters.', to: '/fraud-rings', color: 'text-accent-purple' },
                { icon: 'bar_chart', title: 'Advanced Analytics', desc: 'Predictive modeling using historical data to flag potential risks.', to: '/analytics', color: 'text-accent-red' },
              ].map((mod, i) => (
                <Link key={i} to={mod.to} className="group relative overflow-hidden rounded-[30px] border border-white/8 bg-card-dark/80 p-8 transition-all duration-500 hover:border-white/12">
                  <div className={`absolute -right-20 -top-20 w-64 h-64 bg-gradient-to-br ${i === 0 ? 'from-accent-blue/10' : i === 1 ? 'from-accent-purple/10' : 'from-accent-red/10'} to-transparent blur-[60px] opacity-0 group-hover:opacity-100 transition-opacity duration-500`}></div>

                  <div className="relative z-10">
                    <div className="w-12 h-12 rounded-xl bg-white/5 border border-white/10 flex items-center justify-center mb-6 text-white group-hover:scale-110 transition-transform duration-500">
                      <span className={`material-symbols-outlined text-2xl ${mod.color}`}>{mod.icon}</span>
                    </div>
                    <h3 className="text-xl font-normal text-white mb-3 font-display tracking-wide">{mod.title}</h3>
                    <p className="text-sm text-neutral-400 leading-relaxed mb-6 font-body">{mod.desc}</p>
                    <span className="text-xs text-white opacity-60 group-hover:opacity-100 transition-opacity font-medium flex items-center gap-1">
                      Explore Module <span className="material-symbols-outlined text-sm">arrow_forward</span>
                    </span>
                  </div>
                </Link>
              ))}
            </div>
          </div>
        </section>
      </main>

      <Footer />
    </div>
  )
}
