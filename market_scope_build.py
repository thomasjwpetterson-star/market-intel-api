"""Compile distributed carry checks for private candidates; no AWS side effects.

Full inventories stay in AWS. Python continuity tests are the local oracle;
these SQL checks run over pinned normalized canonical relations in Athena.
`facets` contains mappings, work subjects AND holds, never monetary values.
Transitions/retirements/removals come only from reviewed, source-bound authority.
"""
from dataclasses import dataclass, asdict
from market_scope_membership import KEY, _table, _hash, _literal


@dataclass(frozen=True)
class ScopeCarryRelations:
    previous_facts: str
    current_facts: str
    previous_facets: str
    current_facets: str
    identity_transitions: str
    retired_dispositions: str
    approved_removals: str
    transition_binding_sha256: str
    approved_scope_changes: str = ''

    def __post_init__(self):
        for name, value in asdict(self).items():
            if name == 'approved_scope_changes' and not value:
                continue
            _hash(value) if name == 'transition_binding_sha256' else _table(value)

    def validation_sql(self):
        key_join = lambda left,right: ' AND '.join(f'{left}.{k}={right}.{k}' for k in KEY)
        alias_old = ' AND '.join(f'a.old_{k}=o.{k}' for k in KEY)
        alias_new = ' AND '.join(f'a.current_{k}=n.{k}' for k in KEY)
        projected = ','.join(f'coalesce(a.current_{k},o.{k}) AS {k}' for k in KEY)
        proof = _literal(self.transition_binding_sha256)
        alias_keys = ','.join('old_' + k for k in KEY)
        alias_targets = ','.join('current_' + k for k in KEY)
        native_keys = ','.join(KEY)
        semantic = 'n.kind=o.kind AND n.facet_id=o.facet_id AND n.relationship=o.relationship'
        scopes = (f'SELECT * FROM {self.approved_scope_changes}' if self.approved_scope_changes else
            f"SELECT {native_keys},scope_sha256 AS old_scope_sha256,scope_sha256 AS current_scope_sha256,"
            f"CAST(NULL AS varchar) AS authority_sha256,CAST(NULL AS varchar) AS binding_sha256 FROM {self.previous_facts} WHERE 1=0")
        return f"""WITH scope_changes AS ({scopes}), carried AS (
          SELECT {projected},o.kind,o.facet_id,o.relationship
          FROM {self.previous_facets} o LEFT JOIN {self.identity_transitions} a ON {alias_old}
          WHERE (a.old_native_record_id IS NOT NULL OR NOT EXISTS
            (SELECT 1 FROM {self.retired_dispositions} r WHERE {key_join('r','o')}))
            AND NOT EXISTS (SELECT 1 FROM {self.approved_removals} r
              WHERE {key_join('r','o')} AND r.kind=o.kind AND r.facet_id=o.facet_id AND r.relationship=o.relationship)
        ), issues AS (
          SELECT 'LOST_PRIOR_FACET' reason FROM carried o WHERE NOT EXISTS (
            SELECT 1 FROM {self.current_facets} n WHERE {key_join('n','o')} AND {semantic})
          UNION ALL SELECT 'LOST_NATIVE_WITHOUT_DISPOSITION' FROM {self.previous_facts} o
            WHERE NOT EXISTS (SELECT 1 FROM {self.current_facts} n WHERE {key_join('n','o')})
              AND NOT EXISTS (SELECT 1 FROM {self.identity_transitions} a WHERE {alias_old})
              AND NOT EXISTS (SELECT 1 FROM {self.retired_dispositions} r WHERE {key_join('r','o')})
          UNION ALL SELECT 'STALE_AUTHORITY' FROM {self.identity_transitions}
            WHERE binding_sha256 IS NULL OR binding_sha256<>{proof}
              OR authority_sha256 IS NULL OR NOT regexp_like(authority_sha256,'^[a-f0-9]{{64}}$')
          UNION ALL SELECT 'STALE_AUTHORITY' FROM {self.retired_dispositions}
            WHERE binding_sha256 IS NULL OR binding_sha256<>{proof}
              OR authority_sha256 IS NULL OR NOT regexp_like(authority_sha256,'^[a-f0-9]{{64}}$')
          UNION ALL SELECT 'STALE_AUTHORITY' FROM {self.approved_removals}
            WHERE binding_sha256 IS NULL OR binding_sha256<>{proof}
              OR authority_sha256 IS NULL OR NOT regexp_like(authority_sha256,'^[a-f0-9]{{64}}$')
          UNION ALL SELECT 'STALE_AUTHORITY' FROM scope_changes
            WHERE binding_sha256 IS NULL OR binding_sha256<>{proof}
              OR authority_sha256 IS NULL OR NOT regexp_like(authority_sha256,'^[a-f0-9]{{64}}$')
          UNION ALL SELECT 'UNREVIEWED_SCOPE_CHANGE' FROM {self.previous_facts} o
            JOIN {self.current_facts} n ON {key_join('n','o')}
            WHERE o.scope_sha256<>n.scope_sha256 AND NOT EXISTS (
              SELECT 1 FROM scope_changes r WHERE {key_join('r','o')}
                AND r.old_scope_sha256=o.scope_sha256 AND r.current_scope_sha256=n.scope_sha256)
          UNION ALL SELECT 'INVALID_SCOPE_CHANGE_AUTHORITY' FROM scope_changes r
            WHERE NOT EXISTS (SELECT 1 FROM {self.previous_facts} o JOIN {self.current_facts} n
              ON {key_join('n','o')} WHERE {key_join('r','o')}
                AND r.old_scope_sha256=o.scope_sha256 AND r.current_scope_sha256=n.scope_sha256)
          UNION ALL SELECT 'CONFLICTING_NATIVE_DISPOSITION' FROM {self.retired_dispositions} o
            WHERE EXISTS (SELECT 1 FROM {self.identity_transitions} a WHERE {alias_old})
          UNION ALL SELECT 'AMBIGUOUS_NATIVE_TRANSITION' FROM {self.identity_transitions}
            GROUP BY {alias_keys} HAVING count(*)<>1
          UNION ALL SELECT 'NON_INJECTIVE_NATIVE_TRANSITION' FROM {self.identity_transitions}
            GROUP BY {alias_targets} HAVING count(*)<>1
          UNION ALL SELECT 'INVALID_NATIVE_TRANSITION' FROM {self.identity_transitions} a
            WHERE a.old_source_kind<>a.current_source_kind OR a.old_fiscal_year<>a.current_fiscal_year
              OR a.old_source_record_id<>a.current_source_record_id
              OR EXISTS (SELECT 1 FROM {self.current_facts} o WHERE {alias_old})
              OR NOT EXISTS (SELECT 1 FROM {self.previous_facts} o JOIN {self.current_facts} n
                ON {alias_new} AND n.scope_sha256=o.scope_sha256 WHERE {alias_old})
          UNION ALL SELECT 'INVALID_RETIRED_DISPOSITION' FROM {self.retired_dispositions} r
            WHERE EXISTS (SELECT 1 FROM {self.current_facts} o WHERE {key_join('r','o')})
              OR NOT EXISTS (SELECT 1 FROM {self.previous_facts} o WHERE {key_join('r','o')})
          UNION ALL SELECT 'ORPHAN_CURRENT_FACET' FROM {self.current_facets} o
            WHERE NOT EXISTS (SELECT 1 FROM {self.current_facts} n WHERE {key_join('n','o')})
          UNION ALL SELECT 'ORPHAN_PRIOR_FACET' FROM {self.previous_facets} o
            WHERE NOT EXISTS (SELECT 1 FROM {self.previous_facts} n WHERE {key_join('n','o')})
          UNION ALL SELECT 'DUPLICATE_CURRENT_FINANCIAL_SLICE' FROM {self.current_facts}
            GROUP BY {native_keys} HAVING count(*)<>1
        ) SELECT reason,count(*) AS issues FROM issues GROUP BY reason"""
