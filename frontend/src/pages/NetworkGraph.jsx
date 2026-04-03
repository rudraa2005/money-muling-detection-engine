import { useState, useRef, useEffect, useCallback } from 'react'
import { useSearchParams, Link, useNavigate } from 'react-router-dom'
import Navbar from '../components/Navbar'
import { useAnalysis } from '../context/AnalysisContext'

function inferCurrencyPrefix(analysis) {
  const symbol =
    analysis?.summary?.currency_symbol ||
    analysis?.currency_symbol ||
    analysis?.meta?.currency_symbol ||
    ''

  if (!symbol || symbol.includes('?') || /[^\x00-\x7F]/.test(symbol)) {
    return 'INR '
  }
  return symbol
}

function formatAmount(value, prefix) {
  const num = Number(value)
  if (!Number.isFinite(num)) return String(value || 'N/A')
  return `${prefix}${num.toLocaleString()}`
}

function buildGraphData(analysis, highlightRing, accountFocus) {
  // When analysis is available, derive ring-aware graph data.
  if (analysis?.graph_data?.nodes?.length && analysis.graph_data.edges?.length) {
    const allNodes = []
    const allEdges = []
    const nodeMap = new Map()
    const adjacency = new Map()

    const suspiciousList = analysis.suspicious_accounts || []
    const fraudRings = analysis.fraud_rings || []

    // Map account -> suspicion and ring membership
    const accountMeta = new Map()
    suspiciousList.forEach((a) => {
      accountMeta.set(a.account_id, {
        suspicion: a.suspicion_score,
        ringId: a.ring_id === 'RING_NONE' ? null : a.ring_id,
      })
    })
    const ringRisk = new Map()
    fraudRings.forEach((r) => {
      ringRisk.set(r.ring_id, r.risk_score)
      r.member_accounts.forEach((acct) => {
        const meta = accountMeta.get(acct) || { suspicion: 0, ringId: null }
        accountMeta.set(acct, { ...meta, ringId: r.ring_id })
      })
    })

    const suspicionByAccount = new Map()
    suspiciousList.forEach((s) => {
      suspicionByAccount.set(String(s.account_id), Number(s.suspicion_score) || 0)
    })

    // Build adjacency for account-level navigation
    analysis.graph_data.edges.forEach((edge) => {
      const u = String(edge.source)
      const v = String(edge.target)
      if (!adjacency.has(u)) adjacency.set(u, new Set())
      if (!adjacency.has(v)) adjacency.set(v, new Set())
      adjacency.get(u).add(v)
      adjacency.get(v).add(u)
    })

    // Keep graph clean by default: prioritize top-risk rings + top-risk accounts.
    let allowedAccounts = new Set()
    let highlightedRingMembers = []
    if (highlightRing) {
      const ring = fraudRings.find(r => r.ring_id === highlightRing)
      if (ring) {
        highlightedRingMembers = ring.member_accounts.map(m => String(m))
        allowedAccounts = new Set(highlightedRingMembers)
      }
    } else if (fraudRings.length > 0) {
      const topRings = [...fraudRings]
        .sort((a, b) => (Number(b.risk_score) || 0) - (Number(a.risk_score) || 0))
        .slice(0, 5)
      topRings.forEach((r) => r.member_accounts.forEach((m) => allowedAccounts.add(String(m))))

      const topSuspicious = [...suspiciousList]
        .sort((a, b) => (Number(b.suspicion_score) || 0) - (Number(a.suspicion_score) || 0))
        .slice(0, 25)
      topSuspicious.forEach((s) => allowedAccounts.add(String(s.account_id)))
    } else {
      const topSuspicious = [...suspiciousList]
        .sort((a, b) => (Number(b.suspicion_score) || 0) - (Number(a.suspicion_score) || 0))
        .slice(0, 35)
      topSuspicious.forEach((s) => allowedAccounts.add(String(s.account_id)))
    }

    // If focusing on a specific account, override allowedAccounts with a compact local neighborhood.
    if (accountFocus && !highlightRing) {
      const start = String(accountFocus)
      const visited = new Set([start])
      const queue = [start]
      while (queue.length && visited.size < 35) {
        const cur = queue.shift()
        const nbrs = adjacency.get(cur)
        if (!nbrs) continue
        const sortedNbrs = [...nbrs].sort(
          (a, b) => (suspicionByAccount.get(b) || 0) - (suspicionByAccount.get(a) || 0),
        )
        for (const n of sortedNbrs) {
          if (!visited.has(n)) {
            visited.add(n)
            queue.push(n)
            if (visited.size >= 35) break
          }
        }
      }
      allowedAccounts = visited
    }

    if (highlightRing && accountFocus) {
      // Keep preview mode strict: ring-only graph with focused account highlighted.
      allowedAccounts.add(String(accountFocus))
    }

    // Hard cap to prevent visual overload on very large uploads.
    if (allowedAccounts.size > 70) {
      const ranked = [...allowedAccounts].sort(
        (a, b) => (suspicionByAccount.get(b) || 0) - (suspicionByAccount.get(a) || 0),
      )
      allowedAccounts = new Set(ranked.slice(0, 70))
    }

    const currencyPrefix = inferCurrencyPrefix(analysis)

    // Build one representative transaction per account so node tooltips/panels
    // always have consistent transaction metadata.
    const txByAccount = new Map()
    analysis.graph_data.edges.forEach((edge) => {
      const fromId = String(edge.source)
      const toId = String(edge.target)
      if (allowedAccounts.size && (!allowedAccounts.has(fromId) || !allowedAccounts.has(toId))) return

      const tx = {
        transaction_id: edge.transaction_id || 'N/A',
        amount: formatAmount(edge.amount, currencyPrefix),
        timestamp: edge.timestamp || 'N/A',
        ts: Number.isFinite(new Date(edge.timestamp).getTime()) ? new Date(edge.timestamp).getTime() : -1,
      }

      const setIfBetter = (accountId) => {
        const prev = txByAccount.get(accountId)
        if (!prev || tx.ts > prev.ts) {
          txByAccount.set(accountId, tx)
        }
      }

      setIfBetter(fromId)
      setIfBetter(toId)
    })

    analysis.graph_data.nodes.forEach((node) => {
      const id = String(node.id)
      if (allowedAccounts.size && !allowedAccounts.has(id)) return

      const meta = accountMeta.get(id) || { suspicion: node.risk_score || 0, ringId: null }
      const ringId = meta.ringId
      const ringScore = ringId ? ringRisk.get(ringId) ?? 0 : 0

      // Use a lower threshold (50) for visual distinction, but high threshold for 'flagged' status
      const suspicionScore = meta.suspicion || 0
      const isSuspicious = suspicionScore >= 50 || node.is_suspicious || (node.risk_score || 0) >= 50
      const isHighlySuspicious = suspicionScore >= 75

      if (!nodeMap.has(id)) {
        nodeMap.set(id, {
          globalId: id,
          label: id,
          role: isHighlySuspicious ? 'High Risk' : (isSuspicious ? 'Suspicious' : 'Monitored'),
          // Visual scaling based on risk
          r: isHighlySuspicious ? 12 : (isSuspicious ? 9 : 5),
          color: isHighlySuspicious ? '#b46b58' : (isSuspicious ? '#94836a' : '#5a6573'),
          ringId,
          accId: id,
          txnId: '',
          amount: '',
          timestamp: '',
          ringType: ringId ? fraudRings.find((r) => r.ring_id === ringId)?.pattern_type ?? null : null,
          ringScore,
          isSuspicious,
          suspicionScore,
          txMeta: txByAccount.get(id) || null,
        })
        allNodes.push(nodeMap.get(id))
      }
    })

    analysis.graph_data.edges.forEach((edge) => {
      const fromId = String(edge.source)
      const toId = String(edge.target)
      if (allowedAccounts.size && (!allowedAccounts.has(fromId) || !allowedAccounts.has(toId))) return

      const from = nodeMap.get(fromId)
      const to = nodeMap.get(toId)
      if (!from || !to) return

      const isSuspicious = from.isSuspicious || to.isSuspicious
      const ringId = from.ringId && from.ringId === to.ringId ? from.ringId : null
      allEdges.push({
        from: fromId,
        to: toId,
        suspicious: isSuspicious,
        ringId,
        amount: edge.amount,
        timestamp: edge.timestamp,
        transaction_id: edge.transaction_id,
      })

      // Backfill from edge if tx metadata is still missing.
      if ((!to.txnId || to.txnId === 'N/A') && edge.transaction_id) {
        to.txnId = edge.transaction_id
        to.amount = formatAmount(edge.amount, currencyPrefix)
        to.timestamp = edge.timestamp || 'N/A'
      }
      if ((!from.txnId || from.txnId === 'N/A') && edge.transaction_id) {
        from.txnId = edge.transaction_id
        from.amount = formatAmount(edge.amount, currencyPrefix)
        from.timestamp = edge.timestamp || 'N/A'
      }
    })

    // In ring preview mode, keep the graph focused: only show a compact cycle among ring members.
    if (highlightRing && allowedAccounts.size > 1) {
      const orderedMembers = highlightedRingMembers.length
        ? highlightedRingMembers.filter((id) => allowedAccounts.has(id))
        : [...allowedAccounts].sort(
          (a, b) => (suspicionByAccount.get(b) || 0) - (suspicionByAccount.get(a) || 0),
        )
      const edgeByPair = new Map()
      allEdges.forEach((e) => {
        edgeByPair.set(`${e.from}->${e.to}`, e)
      })

      const cycleEdges = []
      for (let i = 0; i < orderedMembers.length; i++) {
        const from = orderedMembers[i]
        const to = orderedMembers[(i + 1) % orderedMembers.length]
        const direct = edgeByPair.get(`${from}->${to}`)
        const reverse = edgeByPair.get(`${to}->${from}`)
        const chosen = direct || reverse
        cycleEdges.push({
          from,
          to,
          suspicious: true,
          ringId: highlightRing,
          amount: chosen?.amount ?? 0,
          timestamp: chosen?.timestamp ?? '',
          transaction_id: chosen?.transaction_id ?? '',
        })
      }

      allEdges.length = 0
      allEdges.push(...cycleEdges)
    }

    if (allEdges.length > 200) {
      allEdges.sort((a, b) => {
        const aRank = (a.suspicious ? 2 : 0) + (a.ringId ? 1 : 0)
        const bRank = (b.suspicious ? 2 : 0) + (b.ringId ? 1 : 0)
        if (bRank !== aRank) return bRank - aRank
        return (Number(b.amount) || 0) - (Number(a.amount) || 0)
      })
      allEdges.splice(200)
    }

    return { allNodes, allEdges }
  }

  // Minimal fallback demo graph when no analysis is available yet
  const demoNodes = [
    { globalId: 'A', label: 'ACCT-A', role: 'Monitored', r: 6, color: '#5a6573', ringId: null, accId: 'ACCT-A', txnId: 'DEMO-1', amount: 'INR 1,200', timestamp: 'Demo' },
    { globalId: 'B', label: 'ACCT-B', role: 'Suspicious', r: 8, color: '#94836a', ringId: null, accId: 'ACCT-B', txnId: 'DEMO-2', amount: 'INR 8,400', timestamp: 'Demo' },
    { globalId: 'C', label: 'ACCT-C', role: 'Monitored', r: 5, color: '#5a6573', ringId: null, accId: 'ACCT-C', txnId: 'DEMO-3', amount: 'INR 600', timestamp: 'Demo' },
  ]
  const demoEdges = [
    { from: 'A', to: 'B', suspicious: true, ringId: null },
    { from: 'B', to: 'C', suspicious: true, ringId: null },
  ]
  return { allNodes: demoNodes, allEdges: demoEdges }
}

export default function NetworkGraph() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const highlightRing = searchParams.get('ring')
  const accountFocus = searchParams.get('account')
  const { analysis } = useAnalysis()
  const focusedAccountId = accountFocus ? String(accountFocus) : null
  const investigateMode = Boolean(highlightRing || accountFocus)

  const canvasRef = useRef(null)
  const animRef = useRef(null)
  const nodesRef = useRef([])
  const edgesRef = useRef([])
  const hoveredRef = useRef(null)
  const panRef = useRef({ x: 0, y: 0 })
  const zoomRef = useRef(1)
  const dragRef = useRef(null)
  const lastMouseRef = useRef({ x: 0, y: 0 })
  const isPanningRef = useRef(false)
  const sizeRef = useRef({ w: 0, h: 0 })

  const [hoveredNode, setHoveredNode] = useState(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })
  const [selectedNode, setSelectedNode] = useState(null)
  const [zoomLevel, setZoomLevel] = useState(100)
  const [visibleAccountIds, setVisibleAccountIds] = useState([])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const w = canvas.offsetWidth, h = canvas.offsetHeight
    sizeRef.current = { w, h }
    canvas.width = w * 2; canvas.height = h * 2

    const { allNodes, allEdges } = buildGraphData(analysis, highlightRing, accountFocus)
    const cx = w / 2, cy = h / 2
    const ringIds = [...new Set(allNodes.filter(n => n.ringId).map(n => n.ringId))]
    const highlightedRing = highlightRing
      ? (analysis?.fraud_rings || []).find((r) => r.ring_id === highlightRing)
      : null
    const ringMembers = highlightRing
      ? (highlightedRing?.member_accounts || allNodes.filter((n) => n.ringId === highlightRing).map((n) => n.globalId)).map(String)
      : []
    const ringIndex = new Map(ringMembers.map((id, idx) => [id, idx]))
    const clusterPositions = {}
    ringIds.forEach((rid, i) => {
      const angle = (i / ringIds.length) * Math.PI * 2 - Math.PI / 2
      clusterPositions[rid] = {
        x: cx + Math.cos(angle) * Math.min(w, h) * 0.28,
        y: cy + Math.sin(angle) * Math.min(w, h) * 0.28,
      }
    })

    nodesRef.current = allNodes.map((node) => {
      let startX, startY
      if (highlightRing && node.ringId === highlightRing) {
        const idx = ringIndex.get(node.globalId) ?? 0
        const count = Math.max(1, ringMembers.length)
        const angle = (idx / count) * Math.PI * 2 - Math.PI / 2
        const radius = Math.min(w, h) * 0.31
        startX = cx + Math.cos(angle) * radius
        startY = cy + Math.sin(angle) * radius
      } else if (node.ringId && clusterPositions[node.ringId]) {
        const cp = clusterPositions[node.ringId]
        const a = Math.random() * Math.PI * 2
        startX = cp.x + Math.cos(a) * (30 + Math.random() * 60)
        startY = cp.y + Math.sin(a) * (30 + Math.random() * 60)
      } else {
        startX = cx + (Math.random() - 0.5) * w * 0.8
        startY = cy + (Math.random() - 0.5) * h * 0.8
      }
      return {
        ...node,
        x: startX,
        y: startY,
        vx: 0,
        vy: 0,
        targetX: startX,
        targetY: startY,
        isHighlighted: highlightRing && node.ringId === highlightRing,
        isFocused: focusedAccountId ? node.globalId === focusedAccountId : false,
        txnId: node.txMeta?.transaction_id || node.txnId || 'N/A',
        amount: node.txMeta?.amount || node.amount || 'N/A',
        timestamp: node.txMeta?.timestamp || node.timestamp || 'N/A',
      }
    })
    edgesRef.current = allEdges.map(e => ({ ...e, dashOffset: 0 }))
    setVisibleAccountIds(
      allNodes
        .map((n) => String(n.globalId))
        .sort((a, b) => a.localeCompare(b)),
    )

    if (focusedAccountId) {
      const focusedNode = nodesRef.current.find((n) => n.globalId === focusedAccountId)
      if (focusedNode) {
        zoomRef.current = 1.15
        setZoomLevel(115)
        setSelectedNode(focusedNode)
        panRef.current = {
          x: w / 2 - focusedNode.x * zoomRef.current,
          y: h / 2 - focusedNode.y * zoomRef.current,
        }
      } else {
        setSelectedNode(null)
      }
    } else {
      setSelectedNode(null)
      if (highlightRing) {
        zoomRef.current = 1.16
        setZoomLevel(116)
        panRef.current = { x: 0, y: 0 }
      }
    }
  }, [highlightRing, accountFocus, analysis])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    const getNode = (id) => nodesRef.current.find(n => n.globalId === id)

    function tick() {
      const { w, h } = sizeRef.current
      const zoom = zoomRef.current, pan = panRef.current
      ctx.setTransform(2, 0, 0, 2, 0, 0)
      ctx.clearRect(0, 0, w, h)
      const bg = ctx.createRadialGradient(w * 0.5, h * 0.45, 20, w * 0.5, h * 0.45, Math.max(w, h) * 0.75)
      bg.addColorStop(0, 'rgb(23,24,28)')
      bg.addColorStop(1, 'rgb(10,11,13)')
      ctx.fillStyle = bg
      ctx.fillRect(0, 0, w, h)

      ctx.save()
      ctx.translate(pan.x, pan.y)
      ctx.scale(zoom, zoom)

      // Dotted Grid - Enhanced Visibility
      ctx.fillStyle = 'rgba(255, 255, 255, 0.06)'
      const gs = 50 // Grid size
      const sx = Math.floor(-pan.x / zoom / gs) * gs - gs
      const sy = Math.floor(-pan.y / zoom / gs) * gs - gs
      for (let gx = sx; gx < sx + w / zoom + gs * 2; gx += gs) {
        for (let gy = sy; gy < sy + h / zoom + gs * 2; gy += gs) {
          ctx.beginPath(); ctx.arc(gx, gy, 1.0 / zoom, 0, Math.PI * 2); ctx.fill()
        }
      }

      const nodes = nodesRef.current, edges = edgesRef.current
      const hovered = hoveredRef.current, dragging = dragRef.current

      // Physics
      for (let i = 0; i < nodes.length; i++) {
        const n = nodes[i]
        if (dragging === n) continue
        n.vx += (n.targetX - n.x) * (highlightRing ? 0.02 : 0.008)
        n.vy += (n.targetY - n.y) * (highlightRing ? 0.02 : 0.008)
        if (!highlightRing) {
          n.vx += Math.sin(Date.now() * 0.0005 + i * 1.7) * 0.04
          n.vy += Math.cos(Date.now() * 0.0006 + i * 2.3) * 0.03
        }
        for (const edge of edges) {
          let other = null
          if (edge.from === n.globalId) other = getNode(edge.to)
          else if (edge.to === n.globalId) other = getNode(edge.from)
          if (!other) continue
          const dx = other.x - n.x, dy = other.y - n.y
          const dist = Math.sqrt(dx * dx + dy * dy) || 1
          if (dist > 80) { n.vx += (dx / dist) * 0.008; n.vy += (dy / dist) * 0.008 }
        }
        for (let j = i + 1; j < nodes.length; j++) {
          const o = nodes[j]
          const dx = n.x - o.x, dy = n.y - o.y
          const dist = Math.sqrt(dx * dx + dy * dy) || 1
          const minD = (n.r + o.r) * 3
          if (dist < minD) {
            const f = (minD - dist) * 0.004
            n.vx += (dx / dist) * f; n.vy += (dy / dist) * f
            o.vx -= (dx / dist) * f; o.vy -= (dy / dist) * f
          }
        }
        n.vx *= 0.94; n.vy *= 0.94; n.x += n.vx; n.y += n.vy
      }

      // Edges
      for (const edge of edges) {
        const from = getNode(edge.from), to = getNode(edge.to)
        if (!from || !to) continue
        const isRing = edge.ringId === highlightRing
        const isHov = hovered && (hovered.globalId === edge.from || hovered.globalId === edge.to)
        const isFocusedEdge = focusedAccountId && (edge.from === focusedAccountId || edge.to === focusedAccountId)

        // Draw Edge
        ctx.beginPath()
        ctx.moveTo(from.x, from.y)
        ctx.lineTo(to.x, to.y)

        // Monochrome Edge Styling
        const isSuspicious = edge.suspicious
        if (isFocusedEdge) {
          ctx.strokeStyle = isHov ? '#d4dde6' : 'rgba(124, 139, 156, 0.95)'
          ctx.lineWidth = (isHov ? 3.4 : 2.4) / zoom
          ctx.setLineDash([6 / zoom, 4 / zoom])
        } else if (isRing) {
          ctx.strokeStyle = isHov ? '#bcc8d4' : 'rgba(124, 139, 156, 0.82)'
          ctx.lineWidth = (isHov ? 3 : 2.2) / zoom
          ctx.setLineDash([])
        } else if (isSuspicious || isRing) {
          ctx.strokeStyle = isHov ? '#ffffff' : 'rgba(255, 255, 255, 0.9)' // Pure White for highlighted/risk
          ctx.lineWidth = (isHov ? 3 : 2) / zoom
          ctx.setLineDash([5 / zoom, 5 / zoom])
        } else {
          ctx.strokeStyle = isHov ? '#ffffff' : 'rgba(255, 255, 255, 0.10)'
          ctx.lineWidth = (isHov ? 2 : 1) / zoom
          ctx.setLineDash([])
        }

        ctx.stroke()

        // Draw Arrow
        if (isSuspicious || isHov || isRing || isFocusedEdge || zoom > 1.2) {
          const angle = Math.atan2(to.y - from.y, to.x - from.x)
          const headLen = (isHov ? 12 : 8) / zoom
          const r = (to.r * 2.5) + (isHov ? 5 : 0) + (2 / zoom)
          const endX = to.x - r * Math.cos(angle)
          const endY = to.y - r * Math.sin(angle)

          ctx.beginPath()
          ctx.moveTo(endX, endY)
          ctx.lineTo(endX - headLen * Math.cos(angle - Math.PI / 6), endY - headLen * Math.sin(angle - Math.PI / 6))
          ctx.lineTo(endX - headLen * Math.cos(angle + Math.PI / 6), endY - headLen * Math.sin(angle + Math.PI / 6))
          ctx.lineTo(endX, endY)
          ctx.fillStyle = ctx.strokeStyle
          ctx.fill()
        }
        ctx.setLineDash([])
      }

      // Nodes
      for (const n of nodes) {
        const isHov = hovered?.globalId === n.globalId
        const isSel = selectedNode?.globalId === n.globalId
        const isHL = n.isHighlighted // Belongs to the searched ring
        const isFocused = !!n.isFocused

        // size and opacity
        const baseR = n.r * 2.8
        let drawR = baseR * (isHov || isFocused ? 1.35 : 1) / zoom

        const isDimmed = highlightRing && !isHL && !isHov && !isSel && !isFocused
        const isSuspicious = n.isSuspicious || n.suspicionScore > 50

        // 1. Draw Glow (Red for high risk, White for regular interaction)
        if (!isDimmed && (isHov || isHL || isSel || isSuspicious || isFocused)) {
          ctx.beginPath(); ctx.arc(n.x, n.y, drawR + (isHov ? 12 : 8) / zoom, 0, Math.PI * 2)
          if (isFocused) {
            ctx.fillStyle = `rgba(124, 139, 156, ${isHov ? 0.42 : 0.28})`
          } else if (n.role === 'High Risk') {
            ctx.fillStyle = `rgba(180, 107, 88, ${isHov ? 0.38 : 0.24})`
          } else if (n.role === 'Suspicious') {
            ctx.fillStyle = `rgba(148, 131, 106, ${isHov ? 0.34 : 0.22})`
          } else {
            ctx.fillStyle = `rgba(255, 255, 255, ${isHov ? 0.5 : 0.18})`
          }
          ctx.fill()
        }

        if (isFocused) {
          ctx.beginPath()
          ctx.arc(n.x, n.y, drawR + 7 / zoom, 0, Math.PI * 2)
          ctx.strokeStyle = 'rgba(124, 139, 156, 0.92)'
          ctx.lineWidth = 2.2 / zoom
          ctx.stroke()
        }

        // 2. Main Circle Body -> Black or Color tint
        ctx.beginPath()
        ctx.arc(n.x, n.y, drawR, 0, Math.PI * 2)
        ctx.fillStyle = '#000000'
        ctx.globalAlpha = isDimmed ? 0.35 : 1
        ctx.fill()
        ctx.globalAlpha = 1

        // 3. Border - Use data color
        const grad = ctx.createLinearGradient(n.x - drawR, n.y - drawR, n.x + drawR, n.y + drawR)

        if (isDimmed) {
          grad.addColorStop(0, '#333'); grad.addColorStop(1, '#111')
        } else if (isFocused) {
          grad.addColorStop(0, '#9aa9bb'); grad.addColorStop(1, '#5f7084')
        } else if (isHL || isHov || isSel) {
          grad.addColorStop(0, '#ffffff'); grad.addColorStop(1, '#aaaaaa')
        } else if (isSuspicious) {
          grad.addColorStop(0, n.color || '#b46b58'); grad.addColorStop(1, '#5c463f')
        } else {
          grad.addColorStop(0, '#333333'); grad.addColorStop(1, '#111111')
        }

        ctx.strokeStyle = grad
        ctx.lineWidth = (isHov || isHL || isSuspicious || isFocused ? 3.0 : 1.0) / zoom
        ctx.globalAlpha = isDimmed ? 0.35 : 1
        ctx.stroke()
        ctx.globalAlpha = 1

        // Label
        const showLabel = investigateMode || (isSuspicious && zoom >= 0.55) || isHov || isHL || isFocused
        if (!isDimmed && showLabel) {
          ctx.fillStyle = isHov || isHL || isFocused ? '#fff' : '#666'
          ctx.font = `600 ${(isHov ? 10 : 7.5) / zoom}px 'JetBrains Mono', monospace`
          ctx.textAlign = 'center'; ctx.textBaseline = 'middle'
          const fullLabel = String(n.label)
          const labelText = isHov || isFocused ? fullLabel : (fullLabel.length > 10 ? `${fullLabel.slice(0, 10)}...` : fullLabel)
          ctx.fillText(labelText, n.x, n.y)
        }
      }

      ctx.restore()
      animRef.current = requestAnimationFrame(tick)
    }
    animRef.current = requestAnimationFrame(tick)
    return () => { if (animRef.current) cancelAnimationFrame(animRef.current) }
  }, [highlightRing, selectedNode, focusedAccountId, investigateMode])

  const screenToWorld = useCallback((sx, sy) => ({
    x: (sx - panRef.current.x) / zoomRef.current,
    y: (sy - panRef.current.y) / zoomRef.current,
  }), [])

  const worldToScreen = useCallback((wx, wy) => ({
    x: wx * zoomRef.current + panRef.current.x,
    y: wy * zoomRef.current + panRef.current.y,
  }), [])

  const handleMouseMove = useCallback((e) => {
    const canvas = canvasRef.current
    if (!canvas) return
    const rect = canvas.getBoundingClientRect()
    const sx = e.clientX - rect.left, sy = e.clientY - rect.top

    if (dragRef.current) {
      const w = screenToWorld(sx, sy)
      dragRef.current.x = w.x; dragRef.current.y = w.y
      dragRef.current.vx = 0; dragRef.current.vy = 0; return
    }
    if (isPanningRef.current) {
      panRef.current.x += sx - lastMouseRef.current.x
      panRef.current.y += sy - lastMouseRef.current.y
      lastMouseRef.current = { x: sx, y: sy }; canvas.style.cursor = 'grabbing'; return
    }
    lastMouseRef.current = { x: sx, y: sy }
    const w = screenToWorld(sx, sy)
    let found = null
    for (const n of nodesRef.current) {
      const dx = w.x - n.x, dy = w.y - n.y
      if (dx * dx + dy * dy <= (n.r + 4) * (n.r + 4)) { found = n; break }
    }
    hoveredRef.current = found
    if (found) {
      setHoveredNode(found)
      const sp = worldToScreen(found.x, found.y - found.r - 15)
      setTooltipPos(sp); canvas.style.cursor = 'pointer'
    } else { setHoveredNode(null); canvas.style.cursor = 'grab' }
  }, [screenToWorld, worldToScreen])

  const handleMouseDown = useCallback((e) => {
    const rect = canvasRef.current?.getBoundingClientRect()
    if (!rect) return
    lastMouseRef.current = { x: e.clientX - rect.left, y: e.clientY - rect.top }
    if (hoveredRef.current) { dragRef.current = hoveredRef.current; setSelectedNode(hoveredRef.current) }
    else { isPanningRef.current = true }
  }, [])

  const handleMouseUp = useCallback(() => {
    if (dragRef.current) { dragRef.current.targetX = dragRef.current.x; dragRef.current.targetY = dragRef.current.y; dragRef.current = null }
    isPanningRef.current = false
  }, [])

  const handleWheel = useCallback((e) => {
    e.preventDefault()
    const rect = canvasRef.current?.getBoundingClientRect()
    if (!rect) return
    const mx = e.clientX - rect.left, my = e.clientY - rect.top
    const f = e.deltaY > 0 ? 0.92 : 1.08
    const nz = Math.max(0.2, Math.min(4, zoomRef.current * f))
    panRef.current.x = mx - (mx - panRef.current.x) * (nz / zoomRef.current)
    panRef.current.y = my - (my - panRef.current.y) * (nz / zoomRef.current)
    zoomRef.current = nz; setZoomLevel(Math.round(nz * 100))
  }, [])

  useEffect(() => {
    const c = canvasRef.current
    if (!c) return
    c.addEventListener('wheel', handleWheel, { passive: false })
    return () => c.removeEventListener('wheel', handleWheel)
  }, [handleWheel])

  const highlightedRingData = highlightRing
    ? (analysis?.fraud_rings || []).find((r) => r.ring_id === highlightRing)
    : null

  return (
    <div className="bg-[rgb(25,25,25)] text-[rgb(200,200,200)] font-display overflow-hidden h-screen flex flex-col relative">
      <Navbar />
      <main className="flex-1 relative overflow-hidden mt-24 w-full h-[calc(100vh-96px)]">
        {highlightedRingData && (
          <div className="absolute top-4 left-1/2 -translate-x-1/2 z-30 liquid-glass px-5 py-2.5 rounded-full flex items-center gap-3">
            <span className="size-1.5 rounded-full bg-primary animate-pulse"></span>
            <span className="text-xs font-semibold text-white">Investigating #{highlightedRingData.ring_id}</span>
            <span className="text-[10px] text-neutral-500">• {highlightedRingData.pattern_type} •</span>
            <span className="text-[10px] font-semibold text-neutral-300">Score: {Math.round(Number(highlightedRingData.risk_score) || 0)}</span>
            <Link to="/fraud-rings" className="text-[10px] text-neutral-400 hover:text-white transition-colors font-semibold ml-1">← Back</Link>
          </div>
        )}

        {focusedAccountId && (
          <div className="absolute top-4 right-6 z-30 rounded-full border border-accent-blue/30 bg-accent-blue/12 px-4 py-2 text-[11px] font-medium text-white/90">
            Focused account: <span className="font-technical text-white">{focusedAccountId}</span>
          </div>
        )}

        {investigateMode && (
          <aside className="absolute top-14 right-6 z-30 w-[260px] max-h-[55vh] overflow-hidden rounded-2xl border border-white/10 bg-[rgba(12,12,12,0.88)] backdrop-blur-sm">
            <div className="px-4 py-3 border-b border-white/10">
              <p className="text-[10px] uppercase tracking-widest text-neutral-400 font-semibold">Connected IDs</p>
              <p className="text-[11px] text-neutral-300 mt-1">{visibleAccountIds.length} accounts</p>
            </div>
            <div className="max-h-[45vh] overflow-y-auto px-3 py-2">
              {visibleAccountIds.map((id) => (
                <button
                  key={id}
                  type="button"
                  onClick={() =>
                    navigate(
                      `/network-graph?account=${encodeURIComponent(id)}${
                        highlightRing ? `&ring=${encodeURIComponent(highlightRing)}` : ''
                      }`,
                    )
                  }
                  className={`w-full rounded-md px-2.5 py-1.5 text-left font-technical text-[11px] transition-colors ${
                    id === focusedAccountId
                      ? 'border border-accent-blue/40 bg-accent-blue/18 text-white'
                      : 'text-neutral-300 hover:bg-white/10 hover:text-white'
                  }`}
                >
                  {id}
                </button>
              ))}
            </div>
          </aside>
        )}

        <canvas ref={canvasRef} className="absolute inset-0 w-full h-full" style={{ cursor: 'grab' }}
          onMouseMove={handleMouseMove} onMouseDown={handleMouseDown} onMouseUp={handleMouseUp}
          onMouseLeave={() => { hoveredRef.current = null; setHoveredNode(null); isPanningRef.current = false; dragRef.current = null }} />

        {/* Edge Gradients - Blue/Purple/Red */}
        <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-accent-blue via-accent-purple to-accent-red opacity-50 z-20"></div>
        <div className="absolute bottom-0 left-0 right-0 h-24 bg-gradient-to-t from-background-dark to-transparent z-20 pointer-events-none"></div>

        {/* Sidebar */}


        {hoveredNode && (
          <div className="node-tooltip absolute z-50 px-5 py-4 min-w-[280px] pointer-events-none"
            style={{ left: tooltipPos.x, top: tooltipPos.y, transform: 'translate(-50%, -100%)' }}>
            <div className="flex items-center gap-2 mb-2.5">
              <div className="size-2 rounded-full bg-primary"></div>
              <span className="text-[10px] uppercase font-semibold tracking-widest text-[rgb(107,107,107)]">{hoveredNode.role}</span>
              {hoveredNode.ringId && <span className="text-[9px] text-primary font-semibold ml-auto">#{hoveredNode.ringId}</span>}
            </div>
            <p className="text-sm font-bold text-white mb-3 font-technical">{hoveredNode.label}</p>
            <div className="space-y-2 text-xs">
              <div className="flex justify-between gap-6"><span className="text-[rgb(107,107,107)]">Account ID</span><span className="text-white font-technical">{hoveredNode.accId}</span></div>
              <div className="flex justify-between gap-6"><span className="text-[rgb(107,107,107)]">Transaction ID</span><span className="text-white font-technical">{hoveredNode.txnId}</span></div>
              <div className="flex justify-between gap-6"><span className="text-[rgb(107,107,107)]">Amount</span><span className="text-primary font-bold font-technical">{hoveredNode.amount}</span></div>
              <div className="flex justify-between gap-6"><span className="text-[rgb(107,107,107)]">Timestamp</span><span className="text-[rgb(180,180,180)] font-technical">{hoveredNode.timestamp}</span></div>
            </div>
          </div>
        )}

        {selectedNode && (
          <aside className="absolute top-4 left-6 w-[420px] z-30 pointer-events-auto">
            <div className="bg-[rgb(20,20,20)] border border-[rgb(36,36,36)] rounded-2xl overflow-hidden shadow-2xl shadow-black/40">
              <div className="p-5 border-b border-[rgb(36,36,36)]">
                <div className="flex justify-between items-start mb-3">
                  <span className="text-[10px] font-semibold text-neutral-500 uppercase tracking-widest">{selectedNode.role}</span>
                  <button onClick={() => setSelectedNode(null)} className="text-neutral-500 hover:text-white transition-colors">
                    <span className="material-symbols-outlined text-sm">close</span>
                  </button>
                </div>
                <h2 className="text-lg font-bold text-white font-technical">{selectedNode.label}</h2>
                {selectedNode.ringId && <p className="text-[10px] text-neutral-500 font-medium mt-0.5">Ring #{selectedNode.ringId} • {selectedNode.ringType}</p>}
              </div>
              <div className="p-5 space-y-3">
                {[
                  ['Account ID', selectedNode.accId],
                  ['Transaction ID', selectedNode.txnId],
                  ['Amount', selectedNode.amount],
                  ['Timestamp', selectedNode.timestamp],
                  ['Suspicion Score', Number.isFinite(Number(selectedNode.suspicionScore)) ? `${Math.round(Number(selectedNode.suspicionScore))}/100` : 'N/A'],
                ].map(([label, val]) => (
                  <div key={label} className="flex justify-between text-xs">
                    <span className="text-neutral-500">{label}</span>
                    <span className="text-white font-technical">{val}</span>
                  </div>
                ))}
                <div className="pt-3 mt-2 border-t border-[rgb(36,36,36)]">
                  <div className="flex justify-between text-xs">
                    <span className="text-neutral-500">Ring Risk Score</span>
                    <span className="text-primary font-bold text-sm">
                      {Number.isFinite(Number(selectedNode.ringScore)) ? Number(selectedNode.ringScore).toFixed(2) : 'N/A'}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </aside>
        )}

        <div className="absolute bottom-6 left-1/2 -translate-x-1/2 liquid-glass rounded-full px-3.5 py-1.5 flex items-center gap-3 z-20">
          <button onClick={() => { zoomRef.current = Math.max(0.2, zoomRef.current * 0.8); setZoomLevel(Math.round(zoomRef.current * 100)) }}
            className="text-neutral-500 hover:text-white transition-colors"><span className="material-symbols-outlined text-lg">remove</span></button>
          <span className="text-[10px] font-technical text-neutral-500 w-8 text-center">{zoomLevel}%</span>
          <button onClick={() => { zoomRef.current = Math.min(4, zoomRef.current * 1.2); setZoomLevel(Math.round(zoomRef.current * 100)) }}
            className="text-neutral-500 hover:text-white transition-colors"><span className="material-symbols-outlined text-lg">add</span></button>
          <div className="w-px h-3 bg-white/[0.08]"></div>
          <button onClick={() => { zoomRef.current = 1; panRef.current = { x: 0, y: 0 }; setZoomLevel(100) }}
            className="text-neutral-400 hover:text-white transition-colors"><span className="material-symbols-outlined text-lg">center_focus_weak</span></button>
        </div>


      </main>
    </div >
  )
}
