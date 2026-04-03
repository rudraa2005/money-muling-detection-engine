import { Link } from 'react-router-dom'

export default function Footer() {
    const year = new Date().getFullYear()

    return (
        <footer className="mt-auto border-t border-white/8 bg-[linear-gradient(180deg,rgba(18,19,21,0.94),rgba(12,13,15,0.98))]">
            <div className="max-w-7xl mx-auto px-6 py-12">
                <div className="grid gap-10 md:grid-cols-2 lg:grid-cols-4 mb-10">
                    <div>
                        <div className="flex items-center gap-3 mb-4">
                            <div className="flex h-9 w-9 items-center justify-center rounded-2xl border border-white/10 bg-[linear-gradient(145deg,rgba(116,130,150,0.24),rgba(243,239,230,0.08))]">
                                <span className="material-symbols-outlined text-primary text-[16px]">hub</span>
                            </div>
                            <div>
                                <span className="block font-display text-base text-white">GraphLens</span>
                                <span className="block text-[10px] uppercase tracking-[0.2em] text-text-muted">Investigation Workspace</span>
                            </div>
                        </div>
                        <p className="max-w-xs text-sm text-text-muted leading-relaxed">
                            Financial network intelligence workspace for fraud detection, graph analysis, and report review.
                        </p>
                    </div>
                    <div>
                        <h4 className="text-[11px] font-semibold text-text-muted uppercase tracking-widest mb-4">Product</h4>
                        <ul className="space-y-2">
                            <li><Link to="/network-graph" className="text-sm text-text-muted hover:text-white transition-colors">Network Graph</Link></li>
                            <li><Link to="/fraud-rings" className="text-sm text-text-muted hover:text-white transition-colors">Fraud Rings</Link></li>
                            <li><Link to="/analytics" className="text-sm text-text-muted hover:text-white transition-colors">Analytics</Link></li>
                            <li><Link to="/reports" className="text-sm text-text-muted hover:text-white transition-colors">Reports</Link></li>
                            <li><Link to="/history" className="text-sm text-text-muted hover:text-white transition-colors">History</Link></li>
                        </ul>
                    </div>
                    <div>
                        <h4 className="text-[11px] font-semibold text-text-muted uppercase tracking-widest mb-4">Signals</h4>
                        <ul className="space-y-2">
                            <li><span className="text-sm text-text-muted">Behavioral scoring</span></li>
                            <li><span className="text-sm text-text-muted">Graph ring detection</span></li>
                            <li><span className="text-sm text-text-muted">Historical run archive</span></li>
                        </ul>
                    </div>
                    <div>
                        <h4 className="text-[11px] font-semibold text-text-muted uppercase tracking-widest mb-4">System</h4>
                        <ul className="space-y-2">
                            <li><span className="text-sm text-text-muted">FastAPI backend</span></li>
                            <li><span className="text-sm text-text-muted">Vite frontend</span></li>
                            <li><span className="text-sm text-text-muted">Graph + ML pipeline</span></li>
                        </ul>
                    </div>
                </div>
                <div className="border-t border-white/8 pt-5 flex flex-col gap-2 text-[11px] text-[#6d6f71] sm:flex-row sm:items-center sm:justify-between">
                    <p>© {year} GraphLens. Money muling detection workspace.</p>
                    <span>v2.4.1</span>
                </div>
            </div>
        </footer>
    )
}
