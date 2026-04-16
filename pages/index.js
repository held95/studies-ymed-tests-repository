import { useEffect, useState } from 'react'

const fmt = (n) =>
  n == null ? '—' : Number(n).toLocaleString('pt-BR', { maximumFractionDigits: 2 })

const fmtBRL = (n) =>
  n == null
    ? '—'
    : Number(n).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })

function KpiCard({ label, value }) {
  return (
    <div style={styles.card}>
      <div style={styles.cardLabel}>{label}</div>
      <div style={styles.cardValue}>{value}</div>
    </div>
  )
}

export default function Dashboard() {
  const [summary, setSummary] = useState(null)
  const [topProducts, setTopProducts] = useState([])
  const [channels, setChannels] = useState([])
  const [recentOrders, setRecentOrders] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function fetchAll() {
      try {
        const fetchJson = async (url) => {
          const r = await fetch(url)
          if (!r.ok) throw new Error(`${url} retornou status ${r.status}`)
          const text = await r.text()
          try {
            return JSON.parse(text)
          } catch {
            throw new Error(`Resposta inválida de ${url}. Verifique as variáveis de ambiente do Supabase na Vercel.`)
          }
        }
        const [s, tp, ch, ro] = await Promise.all([
          fetchJson('/api/kpi-summary'),
          fetchJson('/api/top-products'),
          fetchJson('/api/revenue-by-channel'),
          fetchJson('/api/recent-orders'),
        ])
        setSummary(s)
        setTopProducts(Array.isArray(tp) ? tp : [])
        setChannels(Array.isArray(ch) ? ch : [])
        setRecentOrders(Array.isArray(ro) ? ro : [])
      } catch (e) {
        setError(e.message)
      } finally {
        setLoading(false)
      }
    }
    fetchAll()
  }, [])

  if (loading) return <div style={styles.center}>Carregando dashboard...</div>
  if (error) return <div style={styles.center}>Erro: {error}</div>

  return (
    <div style={styles.page}>
      <h1 style={styles.title}>YMED Retail Analytics</h1>
      <p style={styles.subtitle}>Dashboard de KPIs — Dados sintéticos (Jan/2024 – Dez/2025)</p>

      {/* KPI Cards */}
      <div style={styles.grid}>
        <KpiCard label="Total de Pedidos"     value={fmt(summary?.total_pedidos)} />
        <KpiCard label="Receita Total"        value={fmtBRL(summary?.receita_total)} />
        <KpiCard label="Ticket Médio"         value={fmtBRL(summary?.ticket_medio)} />
        <KpiCard label="Clientes Ativos"      value={fmt(summary?.clientes_ativos)} />
        <KpiCard label="Produtos Ativos"      value={fmt(summary?.produtos_ativos)} />
        <KpiCard label="Rating Médio"         value={summary?.rating_medio ? `${summary.rating_medio} ★` : '—'} />
        <KpiCard label="Devoluções"           value={fmt(summary?.total_devolucoes)} />
        <KpiCard label="Reclamações Abertas"  value={fmt(summary?.reclamacoes_abertas)} />
      </div>

      {/* Top 5 Produtos */}
      <h2 style={styles.sectionTitle}>Top 5 Produtos por Receita</h2>
      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>Produto</th>
            <th style={styles.th}>Categoria</th>
            <th style={styles.th}>Receita</th>
            <th style={styles.th}>Unidades</th>
          </tr>
        </thead>
        <tbody>
          {topProducts.map((p, i) => (
            <tr key={i} style={i % 2 === 0 ? styles.rowEven : styles.rowOdd}>
              <td style={styles.td}>{p.product_name}</td>
              <td style={styles.td}>{p.product_category}</td>
              <td style={styles.td}>{fmtBRL(p.receita_liquida)}</td>
              <td style={styles.td}>{fmt(p.unidades_vendidas)}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Receita por Canal */}
      <h2 style={styles.sectionTitle}>Receita por Canal</h2>
      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>Canal</th>
            <th style={styles.th}>Pedidos</th>
            <th style={styles.th}>Receita</th>
          </tr>
        </thead>
        <tbody>
          {channels.map((c, i) => (
            <tr key={i} style={i % 2 === 0 ? styles.rowEven : styles.rowOdd}>
              <td style={styles.td}>{c.channel}</td>
              <td style={styles.td}>{fmt(c.total_pedidos)}</td>
              <td style={styles.td}>{fmtBRL(c.receita_liquida)}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Pedidos Recentes */}
      <h2 style={styles.sectionTitle}>Pedidos Recentes</h2>
      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>ID</th>
            <th style={styles.th}>Cliente</th>
            <th style={styles.th}>Data</th>
            <th style={styles.th}>Canal</th>
            <th style={styles.th}>Valor</th>
            <th style={styles.th}>Status</th>
          </tr>
        </thead>
        <tbody>
          {recentOrders.map((o, i) => (
            <tr key={i} style={i % 2 === 0 ? styles.rowEven : styles.rowOdd}>
              <td style={styles.td}>{o.order_id}</td>
              <td style={styles.td}>{o.customer_name}</td>
              <td style={styles.td}>{new Date(o.order_datetime).toLocaleDateString('pt-BR')}</td>
              <td style={styles.td}>{o.channel}</td>
              <td style={styles.td}>{fmtBRL(o.net_amount)}</td>
              <td style={styles.td}>{o.order_status}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <footer style={styles.footer}>
        Powered by Next.js + Supabase · YMED Retail Analytics Study
      </footer>
    </div>
  )
}

const styles = {
  page:         { maxWidth: 960, margin: '0 auto', padding: '2rem 1rem', fontFamily: 'system-ui, sans-serif', color: '#1a1a2e' },
  title:        { fontSize: '1.8rem', fontWeight: 700, marginBottom: '0.25rem' },
  subtitle:     { color: '#555', marginBottom: '2rem' },
  center:       { textAlign: 'center', padding: '4rem', fontFamily: 'system-ui' },
  grid:         { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '1rem', marginBottom: '2.5rem' },
  card:         { background: '#f0f4ff', borderRadius: 8, padding: '1.25rem', boxShadow: '0 1px 4px rgba(0,0,0,0.08)' },
  cardLabel:    { fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: '#666', marginBottom: '0.5rem' },
  cardValue:    { fontSize: '1.5rem', fontWeight: 700 },
  sectionTitle: { fontSize: '1.1rem', fontWeight: 600, margin: '2rem 0 0.75rem' },
  table:        { width: '100%', borderCollapse: 'collapse', marginBottom: '1.5rem', fontSize: '0.875rem' },
  th:           { textAlign: 'left', padding: '0.6rem 0.75rem', borderBottom: '2px solid #ddd', background: '#f8f9fa' },
  td:           { padding: '0.5rem 0.75rem', borderBottom: '1px solid #eee' },
  rowEven:      { background: '#fff' },
  rowOdd:       { background: '#fafafa' },
  footer:       { marginTop: '3rem', textAlign: 'center', fontSize: '0.75rem', color: '#999' },
}
