"""Fail-closed carry gate for a new native membership projection.

Called by the candidate builder, not on each page. Inputs are full inventories
and canonical facets from their pinned checkpoints. This verifies continuity;
it never restores old money, rematches scopes, or creates financial records.
"""
from decimal import Decimal
from market_scope_membership import KEY, SOURCES, _hash


def _inventory(rows):
    result = {}
    for row in rows:
        key = tuple(row[field] for field in KEY)
        if key in result:
            raise ValueError('Duplicate native allocation key')
        if any(value is None or value == '' for value in key):
            raise ValueError('Incomplete native allocation key')
        if key[0] not in SOURCES or type(key[2]) is not int:
            raise ValueError('Invalid native source/year')
        _hash(row['scope_sha256'])
        amount = Decimal(str(row['net_usd']))
        if not amount.is_finite() or amount != amount.quantize(Decimal('.01')):
            raise ValueError('Invalid signed native cents')
        result[key] = (row['scope_sha256'], amount)
    return result


def audit_scope_carry(previous_facts, current_facts, previous_facets, current_facets,
                     *, previous_subjects=(), current_subjects=(), previous_holds=(), current_holds=(),
                     retired=(), identity_transitions=(), approved_removals=(), approved_scope_changes=()):
    """Facets/subjects/holds are (full KEY tuple, immutable semantic tuple) pairs.

    A retired key needs an explicit source disposition. Alias transitions and
    deliberate removals require exact authorities. Amount-only changes have no
    effect on determination or eligibility, including sign changes and zeros.
    """
    old, new = _inventory(previous_facts), _inventory(current_facts)
    retirements = {}
    for item in retired:
        key = tuple(item['key']); _hash(item['authority_sha256'])
        if key not in old or key in new or key in retirements:
            raise ValueError('Invalid retired source disposition')
        retirements[key] = item['authority_sha256']
    transitions, targets = {}, set()
    for item in identity_transitions:
        left, right = tuple(item['old_key']), tuple(item['current_key'])
        _hash(item['authority_sha256'])
        if (left not in old or left in new or left in transitions or right not in new
                or right in targets or left in retirements or old[left][0] != new[right][0]):
            raise ValueError('Invalid or non-injective native identity transition')
        transitions[left] = right; targets.add(right)
    missing = set(old) - set(new) - set(retirements) - set(transitions)
    if missing:
        raise ValueError('Prior native keys disappeared without source dispositions')
    scope_changes = set()
    for item in approved_scope_changes:
        key = tuple(item['key']); _hash(item['authority_sha256'])
        if (key not in old or key not in new or item['old_scope_sha256'] != old[key][0]
                or item['current_scope_sha256'] != new[key][0]):
            raise ValueError('Invalid approved scope transition')
        scope_changes.add(key)
    for left in set(old) & set(new):
        if old[left][0] != new[left][0] and left not in scope_changes:
            # Scope changes require explicit canonical resolution upstream.
            raise ValueError('Changed funded scope requires an explicit source disposition')
    permitted = set()
    for item in approved_removals:
        _hash(item['authority_sha256'])
        permitted.add((item['kind'], tuple(item['key']), tuple(item['semantic'])))
    counts = {}
    def semantic_set(items):
        result = set()
        for key, semantic in items:
            if (not isinstance(key, (list, tuple)) or len(key) != len(KEY)
                    or not isinstance(semantic, (list, tuple)) or not semantic
                    or any(not isinstance(value, str) or not value for value in semantic)):
                raise ValueError('Invalid canonical facet identity')
            result.add((tuple(key), tuple(semantic)))
        return result
    for kind, before, after in [('mapping', previous_facets, current_facets),
                                ('subject', previous_subjects, current_subjects),
                                ('hold', previous_holds, current_holds)]:
        before, after = semantic_set(before), semantic_set(after)
        if any(key not in old for key, _ in before) or any(key not in new for key, _ in after):
            raise ValueError('Canonical facet is outside its pinned native inventory')
        for key, semantic in before:
            if key in retirements:
                continue  # historical disposition remains; no old money inserted
            current_key = transitions.get(key, key)
            if ((current_key, semantic) not in after and
                    (kind, key, semantic) not in permitted):
                raise ValueError(f'Lost prior {kind}; changed dollars do not revoke a determination')
        counts[kind] = {'previous': len(before), 'current': len(after)}
    repriced = sum(old[key][1] != new[transitions.get(key, key)][1]
                   for key in old if key not in retirements)
    return {'status': 'PASSED', 'old_native_slices': len(old), 'current_native_slices': len(new),
            'retired_dispositions': len(retirements), 'reviewed_identity_transitions': len(transitions),
            'repriced_slices_not_reclassified': repriced, 'facets': counts,
            'financial_writes': 0, 'coverage_gain_claimed': False}
