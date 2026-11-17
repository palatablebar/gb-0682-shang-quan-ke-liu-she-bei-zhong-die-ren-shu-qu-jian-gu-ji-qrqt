import sqlite3,json,os
from datetime import datetime,timezone
from pathlib import Path
from app.errors import DomainError

def connection():
    path=Path(os.environ.get('DB_PATH','data/records.db'));path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path,timeout=10);db.row_factory=sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, body TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1, decision TEXT NOT NULL DEFAULT "待复核")')
    db.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS patches (id TEXT PRIMARY KEY, day TEXT NOT NULL, run_id TEXT NOT NULL, content BLOB NOT NULL, received_at TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS seals (id TEXT PRIMARY KEY, day TEXT NOT NULL, seq INTEGER NOT NULL, batch_key TEXT NOT NULL, run_id TEXT NOT NULL, body TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1, decision TEXT NOT NULL DEFAULT "待复核", created_at TEXT NOT NULL, UNIQUE(day,seq), UNIQUE(day,batch_key))')
    return db

def save(run_id, body):
    with connection() as db:
        db.execute('INSERT OR IGNORE INTO runs(id,body) VALUES (?,?)',(run_id,json.dumps(body,ensure_ascii=False)))
        db.execute("INSERT INTO meta(key,value) VALUES ('latest',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(run_id,))
    return get(run_id)

def get(run_id):
    with connection() as db:r=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone()
    if not r:raise DomainError('NOT_FOUND','批次不存在',404)
    body=json.loads(r['body']);body.update(run_id=r['id'],review_version=r['version'],decision=r['decision']);return body

def history():
    with connection() as db:rows=db.execute("SELECT id,version,decision FROM runs ORDER BY CASE WHEN id=(SELECT value FROM meta WHERE key='latest') THEN 0 ELSE 1 END,rowid DESC LIMIT 100").fetchall()
    return [dict(r) for r in rows]

def review(run_id, version, decision):
    if decision not in ('通过','退回'):raise DomainError('DECISION','复核状态无效')
    with connection() as db:
        cur=db.execute('UPDATE runs SET version=version+1,decision=? WHERE id=? AND version=?',(decision,run_id,version))
        if cur.rowcount!=1:
            if not db.execute('SELECT 1 FROM runs WHERE id=?',(run_id,)).fetchone():raise DomainError('NOT_FOUND','批次不存在',404)
            raise DomainError('STALE','复核版本已更新，请刷新',409)
    return get(run_id)

def _now():
    return datetime.now(timezone.utc).isoformat()

def save_patch(patch_id, day, run_id, content):
    """登记一次补发。同一补发（内容哈希相同）重试只登记一次，返回False，不新增版本。"""
    with connection() as db:
        cur=db.execute('INSERT OR IGNORE INTO patches(id,day,run_id,content,received_at) VALUES (?,?,?,?,?)',(patch_id,day,run_id,content,_now()))
        return cur.rowcount==1

def patches(day):
    with connection() as db:
        rows=db.execute('SELECT id,run_id,content,received_at FROM patches WHERE day=? ORDER BY rowid',(day,)).fetchall()
    return [dict(r) for r in rows]

def days():
    with connection() as db:
        rows=db.execute('SELECT day,COUNT(*) AS n FROM patches GROUP BY day ORDER BY day DESC').fetchall()
    return [dict(r) for r in rows]

def save_seal(seal):
    """按(业务日,批次键)幂等封存：同一观测批次重复封存返回既有版本，不新增序号。"""
    with connection() as db:
        existing=db.execute('SELECT body FROM seals WHERE day=? AND batch_key=?',(seal['day'],seal['batch_key'])).fetchone()
        if existing:return _seal_with_review(db,existing['body']),False
        seq=db.execute('SELECT COALESCE(MAX(seq),0)+1 AS s FROM seals WHERE day=?',(seal['day'],)).fetchone()['s']
        seal['seq']=seq;seal['seal_id']='%s-v%d'%(seal['day'],seq)
        db.execute('INSERT INTO seals(id,day,seq,batch_key,run_id,body,created_at) VALUES (?,?,?,?,?,?,?)',
            (seal['seal_id'],seal['day'],seq,seal['batch_key'],seal['run_id'],json.dumps(seal,ensure_ascii=False),seal['created_at']))
        seal.update(review_version=1,decision='待复核')
        return seal,True

def _seal_with_review(db,body):
    seal=json.loads(body)
    r=db.execute('SELECT version,decision FROM seals WHERE id=?',(seal['seal_id'],)).fetchone()
    seal.update(review_version=r['version'],decision=r['decision'])
    return seal

def latest_seal(day):
    with connection() as db:
        r=db.execute('SELECT body FROM seals WHERE day=? ORDER BY seq DESC LIMIT 1',(day,)).fetchone()
        return _seal_with_review(db,r['body']) if r else None

def seals(day):
    with connection() as db:
        rows=db.execute('SELECT id,seq,batch_key,run_id,version,decision,created_at FROM seals WHERE day=? ORDER BY seq DESC',(day,)).fetchall()
    return [dict(r) for r in rows]

def get_seal(seal_id):
    with connection() as db:
        r=db.execute('SELECT body FROM seals WHERE id=?',(seal_id,)).fetchone()
        if not r:raise DomainError('NOT_FOUND','封存不存在',404)
        return _seal_with_review(db,r['body'])

def review_seal(seal_id, version, decision):
    if decision not in ('通过','退回'):raise DomainError('DECISION','复核状态无效')
    with connection() as db:
        cur=db.execute('UPDATE seals SET version=version+1,decision=? WHERE id=? AND version=?',(decision,seal_id,version))
        if cur.rowcount!=1:
            if not db.execute('SELECT 1 FROM seals WHERE id=?',(seal_id,)).fetchone():raise DomainError('NOT_FOUND','封存不存在',404)
            raise DomainError('STALE','复核版本已更新，请刷新',409)
    return get_seal(seal_id)
