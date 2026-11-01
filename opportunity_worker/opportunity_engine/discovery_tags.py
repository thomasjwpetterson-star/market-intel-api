"""Canonical interests over held scope, context, work modes and PSC references.

Tags organize retrieval. Neither an inferred tag nor a user's selected interest
is a statement of company capability, technical equivalence or qualification.
"""
from collections import defaultdict
import hashlib
import json
import re
from .work_vocabulary import SPEC

VERSION = 'opportunity-interests-v1'
AREAS = {'electronics': 'Defence electronics', 'aircraft': 'Aircraft and aviation',
         'industrial': 'Industrial products and manufacturing', 'land': 'Land systems',
         'maritime': 'Maritime systems', 'services': 'Technology and services',
         'space': 'Space systems', 'weapons': 'Weapons and munitions'}
ALIASES = {'area:electronics': ['defense electronics', 'military electronics'],
           'scope:radar': ['radars', 'AESA', 'active electronically scanned array'],
           'scope:electronic-warfare': ['EW', 'electronic countermeasures'],
           'scope:electro-optical': ['EO/IR', 'EOIR', 'infrared'],
           'scope:rf-microwave': ['RF', 'radio frequency', 'microwave'],
           'scope:ndt-inspection': ['NDT', 'nondestructive testing'],
           'work:design_development': ['R&D', 'research and development', 'prototypes']}


def parse_tags(value):
    if value is None or value == '':
        return []
    if not isinstance(value, str) or len(value) > 4096:
        raise ValueError('Use at most 50 canonical interest tags')
    values = value.split(',')
    if len(values) > 50 or any(not re.fullmatch(r'(?:scope|context|area|work|psc):[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}', x) for x in values):
        raise ValueError('Invalid canonical interest tags')
    return list(dict.fromkeys(values))


def _array(value):
    if isinstance(value, list): return value
    try:
        value = json.loads(value or '[]')
        return value if isinstance(value, list) else []
    except (ValueError, TypeError): return []


class TagCatalogue:
    def __init__(self, topics, classifications=()):
        self.records = {}
        self.children = defaultdict(set)
        self.topic_rows = {r['topic_id']: r for r in topics}
        roots = {key:row for key,row in self.topic_rows.items() if row.get('facet')=='navigation'}
        def add(tag_id, label, facet, parent=None, legacy=None, **extra):
            self.records[tag_id] = dict(tag_id=tag_id, label=label, facet=facet,
                parent_tag_id=parent, legacy_topic_id=legacy, selectable=True,
                aliases=ALIASES.get(tag_id, []), **extra)
        for area in sorted(set(roots) | ({r.get('parent_id') for r in topics if r.get('parent_id') not in self.topic_rows} - {None, ''})):
            if area not in AREAS and area not in roots:
                raise ValueError('Unreviewed vocabulary area: ' + area)
            add('area:' + area, roots.get(area,{}).get('label') or AREAS[area], 'area', selection_basis='explicit_work_scope_descendants',
                description='Follow explicitly described work in this area. Platform mentions remain separate.')
        for row in topics:
            facet = row.get('facet')
            if facet not in ('work_scope', 'platform_context', 'mission_context'):
                continue  # Unspecified historical facets are not silently made capabilities.
            namespace = 'scope' if facet == 'work_scope' else 'context'
            tag_id = namespace + ':' + row['topic_id']
            parent = row.get('parent_id')
            if parent in roots:
                parent = 'area:' + parent
            elif parent in self.topic_rows:
                parent = ('scope:' if self.topic_rows[parent].get('facet') == 'work_scope' else 'context:') + parent
            elif parent:
                parent = 'area:' + parent
            add(tag_id, row['label'], 'work_scope' if namespace == 'scope' else 'context', parent,
                row['topic_id'], selection_basis='explicit_work_scope' if namespace == 'scope' else facet,
                description='Source wording describes the work; this does not establish company qualification.' if namespace == 'scope' else 'Source platform or mission context; this is not the delivered work scope.')
        for row in SPEC['work_types']:
            add('work:' + row['id'], row['label'], 'work_type', selection_basis='explicit_work_mode',
                description='An explicitly described work mode, independently of product or platform.')
        for row in classifications:
            code = str(row.get('psc') or '')
            if not re.fullmatch('[A-Z0-9]{1,4}', code): continue
            parent = row.get('parent_code')
            add('psc:' + code, row['label'], 'classification', 'psc:' + parent if parent else None,
                selection_basis='reported_work_classification', source_url=row.get('source_url'),
                active_in_reference=row.get('active_in_reference'),
                description='Reported PSC classification; broader than technical scope or qualification.')
        for key, row in self.records.items():
            if row['parent_tag_id']: self.children[row['parent_tag_id']].add(key)
        # A corrupt hierarchy cannot create unbounded expansion or include its ancestor.
        for key in self.records: self.descendants(key)

    def descendants(self, key):
        found = set()
        pending = [(key, frozenset())]
        while pending:
            node, path = pending.pop()
            if node in path: raise ValueError('Cycle in interest vocabulary')
            found.add(node)
            pending.extend((child, path | {node}) for child in self.children.get(node, ()))
        return sorted(found)

    def validate(self, ids):
        unknown = [key for key in ids if key not in self.records]
        if unknown: raise ValueError('Unknown canonical interest tags: ' + ', '.join(unknown))
        return [self.records[key] for key in ids]

    def expansion(self, key):
        root = self.records[key]
        facet = 'work_scope' if root['facet'] == 'area' else root['facet']
        return [self.records[k] for k in self.descendants(key) if k in self.records
                and self.records[k]['facet'] == facet]

    def search(self, q='', facet='all', limit=100, offset=0):
        if not isinstance(q, str) or len(q) > 100 or facet not in ('all', 'area', 'work_scope', 'context', 'work_type', 'classification'):
            raise ValueError('Invalid tag search')
        if type(limit) is not int or type(offset) is not int or not 1 <= limit <= 100 or not 0 <= offset <= 10000:
            raise ValueError('Invalid tag pagination')
        needle = q.strip().casefold()
        results = [r for r in self.records.values() if (facet == 'all' or r['facet'] == facet)
                   and (needle or facet == 'classification' or r['facet'] != 'classification')
                   and (not needle or needle in ' '.join([r['label'], r['tag_id'], *r['aliases']]).casefold())]
        results.sort(key=lambda r: (not (needle and needle in (r['label'].casefold(), r['tag_id'].casefold(), *[a.casefold() for a in r['aliases']])), r['facet'] != 'area', r['label'].casefold(), r['tag_id']))
        return dict(records=results[offset:offset+limit], total=len(results), has_more=offset+limit < len(results))

    def matched(self, row, selected):
        work = set(_array(row.get('discovery_work_ids_json')))
        context = set(_array(row.get('discovery_context_ids_json')))
        modes = {v.get('work_type') if isinstance(v, dict) else v for v in _array(row.get('discovery_modes_json')) if isinstance(v,(dict,str))}
        try: annotation = json.loads(row.get('discovery_annotation_json') or '{}')
        except (ValueError, TypeError): annotation = {}
        if not isinstance(annotation, dict): annotation = {}
        matches = []
        for tag in self.validate(selected):
            expanded = self.expansion(tag['tag_id'])
            ids = {t['legacy_topic_id'] for t in expanded if t.get('legacy_topic_id')}
            if tag['facet'] in ('work_scope', 'area'): hit = work & ids
            elif tag['facet'] == 'context': hit = context & ids
            elif tag['facet'] == 'work_type': hit = modes & {t['tag_id'].split(':',1)[1] for t in expanded}
            else: hit = {str(row.get('discovery_work_psc') or '')} & {t['tag_id'].split(':',1)[1] for t in expanded}
            if not hit: continue
            evidence = annotation.get('work_types' if tag['facet'] == 'work_type' else 'topics', [])
            passages = [r['passage'] for r in evidence if isinstance(r,dict) and r.get('passage')
                        and r.get('work_type' if tag['facet']=='work_type' else 'topic_id') in hit]
            matches.append(dict(tag_id=tag['tag_id'], label=tag['label'], facet=tag['facet'],
                                basis=tag['selection_basis'], passages=list(dict.fromkeys(passages))[:5]))
        return matches


def fingerprint(records):
    return hashlib.sha256(json.dumps(records, sort_keys=True, default=str).encode()).hexdigest()
