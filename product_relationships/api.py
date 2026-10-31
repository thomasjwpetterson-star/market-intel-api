"""Authenticated, read-only seller/product/source context. No manufacturer revenue inference."""
import datetime
import json
import logging
import re
from fastapi import APIRouter, Depends, HTTPException, Response
from opportunity_intelligence.mount import LazyRepository, verified_application_principal
from . import queries

MODEL = 'exact-product-source-context-v1'
COUNT_KEYS = ('observed_items','selected_catalog_items','other_source_items','ambiguous_items','seller_design_items','unattributed_items','seller_part_references','other_design_sources','procurement_references')
LIMITATIONS = [
    'Catalog references connect products and sources; they do not identify the manufacturer that supplied a particular award.',
    'Another source may be a licensor, related facility or former design owner. It is not automatically a competitor.',
    'Source snapshots may retain historical or conflicting references. Current rights, authorization and delivery sources need independent confirmation.',
]

def number(value):
    try: return max(0, int(value or 0))
    except (ValueError, TypeError): return 0

def array(value):
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
        return parsed if isinstance(parsed, list) else []
    except (ValueError, TypeError): return []

def product(row):
    result = {k: v for k, v in row.items() if not k.endswith('_json') and k not in COUNT_KEYS}
    for key in COUNT_KEYS:
        if key in row: result[key] = number(row[key])
    for key in ('selected_catalog_match','seller_design_reference'):
        if key in result: result[key] = result[key] is True or result[key] == 'true'
    result['part_references'] = [p for p in array(row.get('seller_parts_json')) if isinstance(p,str)]
    result['source_references'] = [p for p in array(row.get('reference_evidence_json')) if isinstance(p,dict) and p.get('cage')]
    return result

class Repository:
    def __init__(self, source=None): self.source = source or LazyRepository()
    def read(self, sql): return self.source.rows(sql)
    def context(self, selected, seller, limit, offset):
        queries.cage(selected)
        if seller: queries.cage(seller)
        queries.pagination(limit,offset)
        sql = queries.portfolio_query(selected,seller,limit,offset) if seller else queries.market_query(selected,self.source.table('company_registry'),limit,offset)
        rows = self.read(sql)
        first = rows[0] if rows else {}
        if not rows and offset:
            head_sql = queries.portfolio_query(selected,seller,1,0) if seller else queries.market_query(selected,self.source.table('company_registry'),1,0)
            head = self.read(head_sql)
            first = head[0] if head else {}
        totals = {key: number(first.get('total_'+key)) for key in COUNT_KEYS[:6]}
        if not seller:
            totals.pop('observed_items',None)
            totals['sellers'] = number(first.get('sellers'))
            totals['seller_item_relationships'] = number(first.get('seller_item_relationships'))
            totals['scope_items'] = number(first.get('scope_items')) if first else len(self.read(queries.scope_query(selected)))
        records = []
        for row in rows:
            if not row.get('seller_cage'): continue
            clean = {k:v for k,v in row.items() if not k.startswith('total_') and k not in ('sellers','seller_item_relationships','scope_items')}
            if seller:
                for key in COUNT_KEYS[:6]: clean.pop(key,None)
                records.append(product(clean))
            else:
                clean.pop('examples_json',None)
                for key in COUNT_KEYS[:6]: clean[key] = number(row.get(key))
                clean['examples'] = [product(p) for p in array(row.get('examples_json')) if isinstance(p,dict)]
                records.append(clean)
        products = records if seller else [item for record in records for item in record['examples']]
        references = [ref for item in products for ref in item['source_references']]
        source_cages = sorted({ref['cage'] for ref in references if isinstance(ref.get('cage'),str) and re.fullmatch('[A-Z0-9]{5}',ref['cage'])})
        names_status = 'available'
        if source_cages:
            try:
                names = {row['cage_code']:row.get('company_name') for row in self.read("SELECT cage_code,company_name FROM "+self.source.table('company_registry')+" WHERE cage_code IN ("+','.join("'"+value+"'" for value in source_cages)+")")}
                for ref in references: ref['company_name'] = names.get(ref['cage'])
            except Exception:
                names_status = 'unavailable'
                logging.warning('Product source names unavailable; exact CAGE references retained')
        total = totals['observed_items'] if seller else totals['sellers']
        return dict(model=MODEL,company_cage=selected,seller_cage=seller,status='available',records=records,totals=totals,limit=limit,offset=offset,has_more=offset+len(records)<total,generated_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_names_status=names_status,scope='seller_part_reference_history' if seller else 'full_available_company_catalog',limitations=LIMITATIONS)
    def references(self,item,limit,offset):
        # One extra row establishes pagination without truncation masquerading as completeness.
        rows = self.read(queries.references_query(item,min(limit+1,100),offset))
        return dict(model=MODEL,niin=queries.niin(item),records=rows[:limit],has_more=len(rows)>limit,limit=limit,offset=offset,limitations=LIMITATIONS)

def create_router(repository=None, verify=verified_application_principal):
    repository = repository or Repository()
    router = APIRouter(prefix='/product-intelligence',dependencies=[Depends(verify)])
    def read(response, callback):
        response.headers['Cache-Control'] = 'private, no-store'
        try: return callback()
        except ValueError as exc: raise HTTPException(400,str(exc))
        except Exception:
            logging.exception('Product relationship read unavailable')
            raise HTTPException(503,'Product source comparison is temporarily unavailable',headers={'Cache-Control':'private, no-store'})
    @router.get('/companies/{company}/market')
    def market(company: str,response: Response,limit: int=25,offset: int=0):
        return read(response,lambda:repository.context(queries.cage(company),None,limit,offset))
    @router.get('/companies/{company}/sellers/{seller}')
    def portfolio(company: str,seller: str,response: Response,limit: int=25,offset: int=0):
        return read(response,lambda:repository.context(queries.cage(company),queries.cage(seller),limit,offset))
    @router.get('/items/{item}/references')
    def references(item: str,response: Response,limit: int=50,offset: int=0):
        def run():
            queries.pagination(limit,offset)
            if limit>99: raise ValueError('Reference page limit is 99')
            return repository.references(item,limit,offset)
        return read(response,run)
    return router

def mount(app): app.include_router(create_router())
