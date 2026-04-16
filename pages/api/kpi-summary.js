import { supabase } from '../../lib/supabase'

export default async function handler(req, res) {
  try {
    const { data, error } = await supabase.rpc('kpi_summary')
    if (error) return res.status(500).json({ error: error.message })
    res.status(200).json(data)
  } catch (e) {
    res.status(500).json({ error: e.message })
  }
}
