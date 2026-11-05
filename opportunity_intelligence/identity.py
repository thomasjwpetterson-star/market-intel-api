"""Finite presentation of the existing registered parent-CAGE resolver."""
import json
import re
import datetime as dt


def _cage(value):
    return value if isinstance(value,str) and re.fullmatch('[A-Z0-9]{5}',value) and value not in {'00000','99999','XXXXX','ZZZZZ'} else None


def present_identity(cage,rows,as_of):
    result={'cage':cage,'as_of':as_of,'source':'published_company_registry_and_parent_resolution'}
    if not rows:return dict(result,status='not_in_registry')
    if len(rows)!=1 or rows[0].get('cage_code')!=cage:raise ValueError('Inconsistent company identity')
    row=rows[0]
    result.update(status='available',legal_name=row.get('company_name'),city=row.get('city'),region=row.get('region'),country=row.get('country'),source_updated_at=row.get('dla_last_update'))
    status=row.get('resolution_status') or 'no_parent_chain_recorded'
    try:path=json.loads(row.get('chain_path_json') or '[]')
    except (ValueError,TypeError):path=None
    root=_cage(row.get('resolved_registry_parent_cage'))
    usable=status in {'held_registered_parent_chain','chain_ends_at_unheld_reference'}
    if usable and (not isinstance(path,list) or not 2<=len(path)<=17 or any(not _cage(c) for c in path) or path[0]!=cage or len(set(path))!=len(path) or path[-1]!=root):
        status='invalid_chain_requires_review';usable=False
    source_day=None
    for fmt in ('%Y-%m-%d','%d-%b-%Y'):
        try:source_day=dt.datetime.strptime(str(row.get('dla_last_update') or '')[:10 if fmt=='%Y-%m-%d' else 11],fmt).date();break
        except ValueError:pass
    try:published_day=dt.date.fromisoformat(str(as_of)[:10])
    except ValueError:published_day=None
    age=(published_day-source_day).days if published_day and source_day else None
    freshness='unknown' if age is None or age<0 else 'historical_reference' if age>730 else 'recent_registry_reference'
    result['parent_relationship']={'status':status,'basis':'registered_parent_cage_chain','source_updated_at':row.get('dla_last_update'),'freshness':freshness,'current_ownership_verified':False,'immediate_parent_cage':_cage(row.get('parent_cage')),'resolved_parent_cage':root if usable else None,'resolved_parent_name':row.get('resolved_parent_name') if usable and status=='held_registered_parent_chain' else None,'chain':path if usable else [],'scope':'Organisational reference; site capabilities and historical financial grouping remain separate.'}
    return result


def lookup_identity(cage):
    from .api import _REQUEST_RELEASE
    # Reuse the existing published read model and its bounded query cache.
    repo=_repository.resolve_repository();manifest=repo.manifest()
    token=_REQUEST_RELEASE.set((repo,manifest))
    try:return repo.company_identity(cage)
    finally:_REQUEST_RELEASE.reset(token)


# AWS clients are created only when the first authenticated read arrives.
from .mount import LazyRepository
_repository=LazyRepository()
