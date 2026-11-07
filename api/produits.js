// API du catalogue partagé : toutes les fiches sont stockées dans Upstash Redis.
const SEED = require('./_seed.js');

const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const CODE = process.env.TEAM_CODE;
const H = 'cs:produits', T = 'cs:corbeille', INIT = 'cs:init', LOG = 'cs:journal';

async function redis(cmds) {
  const r = await fetch(URL_ + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error('redis ' + r.status);
  const out = await r.json();
  return out.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}
const pairs = arr => { const o = []; for (let i = 0; i < (arr || []).length; i += 2) { try { o.push(JSON.parse(arr[i + 1])); } catch (e) {} } return o; };

async function lire() {
  let [items, corbeille] = await redis([['HGETALL', H], ['HGETALL', T]]);
  items = pairs(items);
  if (!items.length) {
    const [premier] = await redis([['SET', INIT, new Date().toISOString(), 'NX']]);
    if (premier === 'OK') {
      const cmd = ['HSET', H];
      SEED.forEach(p => cmd.push(p.id, JSON.stringify(p)));
      await redis([cmd]);
      items = SEED;
    }
  }
  return { items, corbeille: pairs(corbeille) };
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!URL_ || !TOKEN || !CODE) return res.status(500).json({ ok: false, error: 'config', detail: 'Base de données ou TEAM_CODE manquant dans Vercel.' });
  if ((req.headers['x-team-code'] || '') !== CODE) return res.status(401).json({ ok: false, error: 'code' });
  try {
    if (req.method === 'GET') return res.json({ ok: true, ...(await lire()) });
    if (req.method === 'POST') {
      const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {});
      const ops = Array.isArray(body.ops) ? body.ops.slice(0, 500) : [];
      const cmds = [];
      for (const op of ops) {
        if (op.op === 'save' && op.item && op.item.id) {
          cmds.push(['HSET', H, String(op.item.id), JSON.stringify(op.item)]);
        } else if (op.op === 'delete' && op.id) {
          // Suppression douce : la fiche part dans la corbeille, on peut la restaurer.
          const [v] = await redis([['HGET', H, String(op.id)]]);
          if (v) {
            const p = JSON.parse(v); p.supprimeLe = new Date().toISOString(); p.supprimePar = op.par || '';
            cmds.push(['HSET', T, String(op.id), JSON.stringify(p)], ['HDEL', H, String(op.id)]);
          }
        } else if (op.op === 'restore' && op.id) {
          const [v] = await redis([['HGET', T, String(op.id)]]);
          if (v) {
            const p = JSON.parse(v); delete p.supprimeLe; delete p.supprimePar;
            cmds.push(['HSET', H, String(op.id), JSON.stringify(p)], ['HDEL', T, String(op.id)]);
          }
        }
        cmds.push(['LPUSH', LOG, JSON.stringify({ t: new Date().toISOString(), op: op.op, id: op.id || (op.item && op.item.id), par: op.par || (op.item && op.item.majPar) || '' })]);
      }
      if (cmds.length) { cmds.push(['LTRIM', LOG, 0, 1999]); await redis(cmds); }
      return res.json({ ok: true });
    }
    res.setHeader('Allow', 'GET, POST');
    return res.status(405).json({ ok: false, error: 'method' });
  } catch (e) {
    return res.status(500).json({ ok: false, error: 'server', detail: String(e.message || e) });
  }
};
