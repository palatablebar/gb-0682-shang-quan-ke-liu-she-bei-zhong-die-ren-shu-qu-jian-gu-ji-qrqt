from flask import Blueprint,jsonify,request,Response,render_template
from app import store
from app.config import TITLE,SHORT,MODULES
from app.dayreport import ingest,report,seal,export_csv,days_overview
from app.errors import DomainError
from app.web import payload

bp=Blueprint('dayreport',__name__)

@bp.post('/api/dayreport/ingest')
def ingest_api():
    content,parameters=payload()
    return jsonify(ingest(content,parameters))

@bp.get('/api/dayreport')
def days_api():
    return jsonify(days_overview())

@bp.get('/api/dayreport/seals/<seal_id>')
def seal_detail(seal_id):
    return jsonify(store.get_seal(seal_id))

@bp.post('/api/dayreport/seals/<seal_id>/review')
def seal_review(seal_id):
    data=request.get_json(silent=True) or {}
    if not isinstance(data.get('version'),int):raise DomainError('VERSION','请提交复核版本')
    return jsonify(store.review_seal(seal_id,data['version'],data.get('decision')))

@bp.get('/api/dayreport/seals/<seal_id>/export.csv')
def seal_export(seal_id):
    return Response(export_csv(seal_id),mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition':'attachment; filename=seal-evidence.csv'})

@bp.get('/api/dayreport/<day>')
def report_api(day):
    return jsonify(report(day))

@bp.post('/api/dayreport/<day>/seal')
def seal_api(day):
    saved,created=seal(day)
    return jsonify({'seal':saved,'created':created}),201 if created else 200

@bp.get('/dayreport')
@bp.get('/dayreport/<day>')
def screen(day=None):
    known=days_overview()
    if day is None and known:day=known[0]['day']
    rep=report(day) if day else None
    latest=rep['latest_seal'] if rep else None
    run_view={'decision':latest['decision'],'review_version':latest['review_version']} if latest else {'decision':'未封存','review_version':0}
    empty={'run_id':'—','manifest':{'generated_at':'','rule_version':''},'input_count':0,'rows':[],'quarantine':[],'business':None}
    return render_template('dayreport.html',title=TITLE,short=SHORT,modules=MODULES,current=8,
        section='日报封存',page_kind='dayreport',
        api_link='/api/dayreport/'+day if day else '/api/dayreport',
        data=rep['run'] if rep else empty,run=run_view,report=rep,days=known,day=day)
