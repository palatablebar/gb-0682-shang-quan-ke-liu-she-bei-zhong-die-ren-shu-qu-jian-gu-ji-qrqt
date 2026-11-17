"""日报封存：按期封存 + 补发增量吸收。

裁决：截点一到即以当前观测批次封存正式日报，不等设备补发到齐。
晚到的补发合并进当日观测批次后重放同一套采信规则，但只有结论发生
变化的时段被标为待重核，未覆盖时段沿用上一封存的已核结论。同一补发
重试（内容哈希相同）幂等，不新增版本。每次封存记录所采用的补发范围
与待重核范围，核对页面、日汇总与导出证据始终读同一批观测。
"""
import csv,hashlib,io,json
from collections import defaultdict
from datetime import datetime,timedelta,timezone
from app.config import RULE_VERSION
from app.errors import DomainError
from app.modules.m1_ingest.decode import decode
from app.modules.m7_archive.export_out import bundle as run_bundle
from app.workflow import run
from app import store

SHANGHAI=timezone(timedelta(hours=8),name='Asia/Shanghai')

def _shanghai(bucket):
    return datetime.fromisoformat(bucket).astimezone(SHANGHAI).date().isoformat()

def business_day(state):
    """业务日按 Asia/Shanghai 日历判定；一次上传只允许覆盖一个业务日。

    采纳行为空时（例如整批补发被判冲突）回退到隔离记录里可定位的时段。
    """
    days={_shanghai(r['bucket']) for r in state['rows']}
    if not days:
        for q in state['quarantine']:
            bucket=_quarantine_bucket(q)
            if bucket:days.add(_shanghai(bucket))
    if not days:raise DomainError('DAY','采纳观测为空，无法确定业务日')
    if len(days)>1:raise DomainError('DAY_SPAN','观测跨多个业务日，请按日拆分后分别上传')
    return days.pop()

def merge_contents(contents):
    """把基准与各次补发合并为一份观测文本，行序即到达顺序，重放同一套采信规则。"""
    if len(contents)==1:return contents[0]
    header=None;rows=[]
    for content in contents:
        text,_=decode(content)
        reader=csv.reader(io.StringIO(text,newline=''))
        try:head=[s.strip().lstrip('\ufeff') for s in next(reader)]
        except StopIteration:raise DomainError('EMPTY_FILE','文件为空')
        if header is None:header=head
        elif head!=header:raise DomainError('HEADER','补发表头与基准不一致')
        rows.extend(reader)
    out=io.StringIO(newline='');writer=csv.writer(out);writer.writerow(header);writer.writerows(rows)
    return ('\ufeff'+out.getvalue()).encode('utf-8')

def _quarantine_bucket(entry):
    raw=entry.get('raw')
    value=isinstance(raw,dict) and raw.get('bucket')
    if not value:return None
    try:parsed=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    except ValueError:return None
    if parsed.tzinfo is None:return None
    return parsed.astimezone(timezone.utc).replace(minute=0,second=0,microsecond=0).isoformat()

def fingerprints(state):
    """每个时段的结论指纹：选中观测、计算结果与该时段的隔离记录。

    指纹一致即结论一致，可安全沿用已核结论；任一成分变化则该时段待重核。
    """
    obs=defaultdict(list)
    for r in state['observations']:
        obs[r['bucket']].append([r['id'],str(r['version']),r['kind'],r['left'],r.get('right',''),r['count']])
    calc={c['id']:c for c in state.get('calculations',[])}
    quar=defaultdict(list)
    for q in state['quarantine']:
        bucket=_quarantine_bucket(q)
        if bucket:quar[bucket].append([q['code'],str(q.get('id',''))])
    out={}
    for bucket in set(obs)|set(calc)|set(quar):
        c=calc.get(bucket)
        out[bucket]=json.dumps({'obs':sorted(obs.get(bucket,[])),
            'calc':[c['lower'],c['upper'],c['code']] if c else None,
            'quar':sorted(quar.get(bucket,[]))},sort_keys=True,ensure_ascii=False)
    return out

def diff_buckets(previous_fp,current_fp):
    """结论发生变化的时段（新增、观测变化、被隔离或失效）。"""
    return sorted(b for b in set(previous_fp)|set(current_fp) if previous_fp.get(b)!=current_fp.get(b))

def _coverage(solo):
    """单次补发触及的时段（采纳行与可定位的隔离行）。"""
    buckets={r['bucket'] for r in solo['rows']}
    for q in solo['quarantine']:
        bucket=_quarantine_bucket(q)
        if bucket:buckets.add(bucket)
    return sorted(buckets)

def _batch_key(day,patches):
    payload={'day':day,'patches':[p['id'] for p in patches],'rule':RULE_VERSION}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest()[:20]

def _digest(body):
    """封存指纹：覆盖语义内容；序号与封存ID由存储层分配，不参与摘要。"""
    payload={k:v for k,v in body.items() if k not in ('sha256','seal_id','seq','review_version','decision')}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

def current_batch(day):
    """当日当前观测批次：基准+全部已登记补发合并后重放，核对与日汇总共用。"""
    patches=store.patches(day)
    if not patches:raise DomainError('NOT_FOUND','业务日尚无观测批次',404)
    merged=merge_contents([p['content'] for p in patches])
    state=run(merged,{})
    summaries=[]
    for i,p in enumerate(patches):
        solo=store.get(p['run_id'])
        summaries.append({'seq':i+1,'patch_id':p['id'],'kind':'基准' if i==0 else '补发',
            'run_id':p['run_id'],'sha256':hashlib.sha256(p['content']).hexdigest(),
            'received_at':p['received_at'],'rows':solo['input_count'],'buckets':_coverage(solo)})
    state['manifest']['sources']=summaries
    store.save(state['run_id'],state)
    return state,summaries,patches

def _build_seal_body(day,batch_key,state,summaries,previous):
    current_fp=fingerprints(state)
    previous_fp=previous['fingerprints'] if previous and previous.get('rule_version')==RULE_VERSION else {}
    changed=diff_buckets(previous_fp,current_fp)
    calc={c['id']:c for c in state.get('calculations',[])}
    carried={b['bucket']:b for b in previous['buckets']} if previous else {}
    buckets=[]
    for bucket in sorted(set(current_fp)|set(previous_fp)):
        if bucket in changed:
            c=calc.get(bucket)
            buckets.append({'bucket':bucket,'status':'待重核','carried':False,
                'lower':c['lower'] if c else None,'upper':c['upper'] if c else None,
                'code':c['code'] if c else ('已失效' if bucket not in current_fp else '隔离')})
        else:
            p=carried[bucket]
            buckets.append({'bucket':bucket,'status':'已核沿用','carried':True,
                'lower':p['lower'],'upper':p['upper'],'code':p['code']})
    body={'seal_id':None,'day':day,'seq':None,'batch_key':batch_key,'run_id':state['run_id'],
        'rule_version':RULE_VERSION,'created_at':datetime.now(timezone.utc).isoformat(),
        'patch_range':summaries,'pending_buckets':changed,'buckets':buckets,
        'totals':{'total':sum(b['lower'] for b in buckets if b['lower'] is not None),
            'upper_total':sum(b['upper'] for b in buckets if b['upper'] is not None),
            'buckets':len(buckets),'unit':'客次'},
        'reconcile':state['reconcile'],'fingerprints':current_fp}
    body['sha256']=_digest(body)
    return body

def _seal_view(seal):
    if not seal:return None
    return {k:v for k,v in seal.items() if k!='fingerprints'}

def ingest(content,parameters=None):
    """上传基准或补发：首日上传成为基准，之后同业务日的上传均为补发。"""
    parameters=parameters or {}
    state=run(content,parameters)
    day=business_day(state)
    existing=store.patches(day)
    if existing and not any(p['id']==state['run_id'] for p in existing):
        merge_contents([p['content'] for p in existing]+[content])
    store.save(state['run_id'],state)
    applied=store.save_patch(state['run_id'],day,state['run_id'],content)
    return report(day,ingested={'patch_id':state['run_id'],'applied':applied,'deduped':not applied})

def report(day,ingested=None):
    """核对页面与日汇总共用的视图：当前批次、最新封存与未封存变化预览。"""
    state,summaries,patches=current_batch(day)
    previous=store.latest_seal(day)
    previous_fp=previous['fingerprints'] if previous and previous.get('rule_version')==RULE_VERSION else {}
    pending=diff_buckets(previous_fp,fingerprints(state))
    sealed_ids={p['patch_id'] for p in previous['patch_range']} if previous else set()
    unsealed=[s for s in summaries if s['patch_id'] not in sealed_ids]
    return {'day':day,'run':state,'patches':summaries,
        'latest_seal':_seal_view(previous),'seals':store.seals(day),
        'preview':{'pending_buckets':pending,'unsealed_patches':unsealed,'sealable':bool(unsealed)},
        'ingested':ingested}

def seal(day):
    """封存当前观测批次为正式日报。同批次重复封存幂等，不新增版本。"""
    state,summaries,patches=current_batch(day)
    previous=store.latest_seal(day)
    body=_build_seal_body(day,_batch_key(day,patches),state,summaries,previous)
    saved,created=store.save_seal(body)
    return _seal_view(saved),created

def export_csv(seal_id):
    """封存证据：补发范围、待重核范围、时段结论、日汇总与对账，连同运行证据。"""
    seal=store.get_seal(seal_id)
    state=store.get(seal['run_id'])
    out=io.StringIO(newline='');writer=csv.writer(out)
    writer.writerow(['记录类型','批次','来源行','业务ID','原因码','内容'])
    writer.writerow(['封存',seal_id,'','','',json.dumps({'day':seal['day'],'seq':seal['seq'],
        'batch_key':seal['batch_key'],'rule_version':seal['rule_version'],'created_at':seal['created_at'],
        'sha256':seal['sha256'],'run_id':seal['run_id']},ensure_ascii=False,sort_keys=True)])
    for p in seal['patch_range']:
        writer.writerow(['补发范围',seal_id,'','',p['kind']+str(p['seq']),json.dumps(p,ensure_ascii=False,sort_keys=True)])
    for bucket in seal['pending_buckets']:
        writer.writerow(['待重核范围',seal_id,'',bucket,'待重核',''])
    for b in seal['buckets']:
        writer.writerow(['时段结论',seal_id,'',b['bucket'],b['status'],
            json.dumps({'lower':b['lower'],'upper':b['upper'],'code':b['code'],'carried':b['carried']},ensure_ascii=False,sort_keys=True)])
    writer.writerow(['日汇总',seal_id,'','','',json.dumps(seal['totals'],ensure_ascii=False,sort_keys=True)])
    lines=run_bundle(state).lstrip('\ufeff').splitlines(keepends=True)
    out.write(''.join(lines[1:]))
    return '\ufeff'+out.getvalue()

def days_overview():
    out=[]
    for d in store.days():
        seal=store.latest_seal(d['day'])
        out.append({'day':d['day'],'patches':d['n'],'sealed':bool(seal),
            'seal':{'seal_id':seal['seal_id'],'seq':seal['seq'],'decision':seal['decision']} if seal else None})
    return out
