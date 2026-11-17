import pytest
from app.errors import DomainError

BASE = """id,bucket,version,kind,left,right,count,unit,note
b1,2026-11-16T09:00:00+08:00,1,zone,A,,100,人,基准
b2,2026-11-16T09:00:00+08:00,1,zone,B,,80,人,基准
b3,2026-11-16T09:00:00+08:00,1,pair,A,B,30,人,基准
b4,2026-11-16T10:00:00+08:00,1,zone,A,,100,人,基准
b5,2026-11-16T10:00:00+08:00,1,zone,B,,80,人,基准
b6,2026-11-16T10:00:00+08:00,1,pair,A,B,30,人,基准
"""

PATCH1 = """id,bucket,version,kind,left,right,count,unit,note
p1,2026-11-16T10:00:00+08:00,2,zone,A,,120,人,补发
p2,2026-11-16T11:00:00+08:00,1,zone,A,,90,人,补发
p3,2026-11-16T11:00:00+08:00,1,zone,B,,70,人,补发
p4,2026-11-16T11:00:00+08:00,1,pair,A,B,20,人,补发
"""

PATCH_CONFLICT = """id,bucket,version,kind,left,right,count,unit,note
c1,2026-11-16T09:00:00+08:00,2,zone,A,,130,人,冲突
c2,2026-11-16T09:00:00+08:00,2,zone,A,,131,人,冲突
c3,2026-11-16T09:00:00+08:00,2,zone,B,,85,人,冲突
c4,2026-11-16T09:00:00+08:00,2,zone,B,,86,人,冲突
"""

DAY2 = """id,bucket,version,kind,left,right,count,unit,note
n1,2026-11-17T09:00:00+08:00,1,zone,A,,50,人,次日
"""

SPAN = """id,bucket,version,kind,left,right,count,unit,note
s1,2026-11-16T23:00:00+08:00,1,zone,A,,10,人,跨日
s2,2026-11-17T01:00:00+08:00,1,zone,A,,10,人,跨日
"""

BAD_HEADER = """id,bucket,version,kind,left,right,count,unit,extra
x1,2026-11-16T12:00:00+08:00,1,zone,A,,10,人,异构
"""

B0900 = '2026-11-16T01:00:00+00:00'
B1000 = '2026-11-16T02:00:00+00:00'
B1100 = '2026-11-16T03:00:00+00:00'


def ingest(client, csv_text, status=200):
    r = client.post('/api/dayreport/ingest', json={'csv': csv_text})
    assert r.status_code == status, r.get_json()
    return r.get_json()


def seal_now(client, day='2026-11-16'):
    r = client.post('/api/dayreport/%s/seal' % day)
    assert r.status_code in (200, 201), r.get_json()
    return r.get_json()


def bucket_of(seal, bucket):
    return next(b for b in seal['buckets'] if b['bucket'] == bucket)


def test_base_ingest_creates_day(client):
    rep = ingest(client, BASE)
    assert rep['day'] == '2026-11-16' and rep['ingested']['applied'] is True
    assert [p['kind'] for p in rep['patches']] == ['基准']
    assert rep['run']['reconcile']['balanced'] is True
    assert rep['latest_seal'] is None and rep['preview']['sealable'] is True


def test_patch_retry_no_new_version(client):
    ingest(client, BASE)
    ingest(client, PATCH1)
    again = ingest(client, PATCH1)
    assert again['ingested']['applied'] is False and again['ingested']['deduped'] is True
    assert len(again['patches']) == 2
    first = seal_now(client)
    retry = ingest(client, PATCH1)
    assert len(retry['patches']) == 2 and retry['preview']['sealable'] is False
    second = seal_now(client)
    assert second['created'] is False and second['seal']['seal_id'] == first['seal']['seal_id']
    assert len(retry['seals']) == 1


def test_seal_idempotent_same_batch(client):
    ingest(client, BASE)
    first = seal_now(client)
    assert first['created'] is True and first['seal']['seq'] == 1
    second = seal_now(client)
    assert second['created'] is False and second['seal']['seal_id'] == first['seal']['seal_id']
    assert client.get('/api/dayreport/2026-11-16').get_json()['seals'].__len__() == 1


def test_first_seal_all_pending_then_review(client):
    ingest(client, BASE)
    seal = seal_now(client)['seal']
    assert seal['pending_buckets'] == [B0900, B1000]
    assert all(b['status'] == '待重核' for b in seal['buckets'])
    assert seal['totals'] == {'total': 300, 'upper_total': 300, 'buckets': 2, 'unit': '客次'}
    r = client.post('/api/dayreport/seals/%s/review' % seal['seal_id'], json={'version': 1, 'decision': '通过'})
    assert r.status_code == 200 and r.get_json()['review_version'] == 2 and r.get_json()['decision'] == '通过'


def test_changed_buckets_pending_uncovered_carried(client):
    ingest(client, BASE)
    seal_now(client)
    rep = ingest(client, PATCH1)
    assert rep['preview']['pending_buckets'] == [B1000, B1100]
    seal = seal_now(client)['seal']
    assert seal['seq'] == 2 and seal['pending_buckets'] == [B1000, B1100]
    carried = bucket_of(seal, B0900)
    assert carried['status'] == '已核沿用' and carried['carried'] is True
    assert (carried['lower'], carried['upper']) == (150, 150)
    changed = bucket_of(seal, B1000)
    assert changed['status'] == '待重核' and (changed['lower'], changed['upper']) == (170, 170)
    added = bucket_of(seal, B1100)
    assert added['status'] == '待重核' and (added['lower'], added['upper']) == (140, 140)
    assert seal['totals']['total'] == 460 and seal['totals']['upper_total'] == 460


def test_seal_records_patch_range_and_pending(client):
    ingest(client, BASE)
    ingest(client, PATCH1)
    seal = seal_now(client)['seal']
    kinds = [(p['kind'], p['seq']) for p in seal['patch_range']]
    assert kinds == [('基准', 1), ('补发', 2)]
    coverage = {p['seq']: p['buckets'] for p in seal['patch_range']}
    assert coverage[1] == [B0900, B1000] and coverage[2] == [B1000, B1100]
    assert seal['pending_buckets'] == [B0900, B1000, B1100]
    assert len(seal['sha256']) == 64


def test_reconcile_and_summary_share_one_batch(client):
    ingest(client, BASE)
    rep = ingest(client, PATCH1)
    seal = seal_now(client)['seal']
    assert seal['run_id'] == rep['run']['run_id']
    assert seal['reconcile']['input'] == rep['run']['reconcile']['input'] == 10
    assert seal['totals']['total'] == rep['run']['reconcile']['business']['total'] == 460
    page = client.get('/m6_reconcile')
    assert page.status_code == 200 and '460' in page.data.decode()
    daily = client.get('/dayreport')
    assert daily.status_code == 200 and '460' in daily.data.decode()


def test_conflict_patch_marks_bucket_quarantined(client):
    ingest(client, BASE)
    ingest(client, PATCH1)
    seal_now(client)
    rep = ingest(client, PATCH_CONFLICT)
    assert rep['ingested']['applied'] is True
    seal = seal_now(client)['seal']
    assert seal['pending_buckets'] == [B0900]
    lost = bucket_of(seal, B0900)
    assert lost['status'] == '待重核' and lost['code'] == '隔离' and lost['lower'] is None
    assert bucket_of(seal, B1000)['status'] == '已核沿用'
    assert seal['totals'] == {'total': 310, 'upper_total': 310, 'buckets': 3, 'unit': '客次'}


def test_seal_review_optimistic_lock(client):
    ingest(client, BASE)
    seal = seal_now(client)['seal']
    ok = client.post('/api/dayreport/seals/%s/review' % seal['seal_id'], json={'version': 1, 'decision': '通过'})
    assert ok.status_code == 200 and ok.get_json()['review_version'] == 2
    stale = client.post('/api/dayreport/seals/%s/review' % seal['seal_id'], json={'version': 1, 'decision': '退回'})
    assert stale.status_code == 409 and stale.get_json()['code'] == 'STALE'
    missing = client.post('/api/dayreport/seals/absent/review', json={'version': 1, 'decision': '通过'})
    assert missing.status_code == 404


def test_day_span_rejected(client):
    r = ingest(client, SPAN, status=422)
    assert r['code'] == 'DAY_SPAN'
    assert client.get('/api/dayreport').get_json() == []


def test_cross_day_patch_forms_own_day(client):
    ingest(client, BASE)
    ingest(client, DAY2)
    days = client.get('/api/dayreport').get_json()
    assert [d['day'] for d in days] == ['2026-11-17', '2026-11-16']
    rep = client.get('/api/dayreport/2026-11-17').get_json()
    assert [p['kind'] for p in rep['patches']] == ['基准']


def test_header_mismatch_patch_rejected(client):
    ingest(client, BASE)
    r = ingest(client, BAD_HEADER, status=422)
    assert r['code'] == 'HEADER'
    rep = client.get('/api/dayreport/2026-11-16').get_json()
    assert len(rep['patches']) == 1


def test_export_contains_ranges(client):
    ingest(client, BASE)
    seal_now(client)
    ingest(client, PATCH1)
    seal = seal_now(client)['seal']
    r = client.get('/api/dayreport/seals/%s/export.csv' % seal['seal_id'])
    assert r.status_code == 200
    text = r.data.decode('utf-8-sig')
    for marker in ('封存', '补发范围', '待重核范围', '时段结论', '日汇总', '对账', seal['seal_id'], B1000, B1100, '已核沿用', '待重核'):
        assert marker in text


def test_pages_reachable(client):
    assert client.get('/dayreport').status_code == 200
    ingest(client, BASE)
    seal_now(client)
    page = client.get('/dayreport')
    assert page.status_code == 200
    html = page.data.decode()
    assert '日汇客次' in html and '待重核' in html and '补发范围' in html and '待重核范围' in html
