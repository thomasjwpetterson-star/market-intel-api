"""Select overlapping markets from existing native financial slices.

Engine-first implementation component, not an enabled API or release. The ETL
must resolve its active canonical decisions/holds onto current approved slices
before calling project_memberships. Amounts and entity metadata stay in facts.
Public aggregates and Professional detail must use the SAME selected relation.
The primary allocation/cascade continues to read the original facts unchanged.
"""
from dataclasses import asdict, dataclass
from hashlib import sha256
import json
import re

KEY = ('source_kind', 'source_record_id', 'fiscal_year', 'native_record_id', 'allocation_id')
SOURCES = frozenset(('PRIME_AWARD', 'FIRST_TIER_REPORT', 'DLA_AWARD'))
FACT_COLUMNS = frozenset((*KEY, 'scope_sha256', 'market_node', 'net_usd'))
MEMBERSHIP_COLUMNS = frozenset((*KEY, 'scope_sha256', 'target_dimension', 'target_id', 'relationship',
    'decision_id', 'decision_revision', 'extent_sha256', 'evidence_sha256', 'binding_sha256'))
IDENTIFIER = re.compile(r'[a-z][a-z0-9_]{0,120}\Z')
MARKET = re.compile(r'(?:[A-Z][A-Z0-9_]{0,120}|domain:(?:air|naval|ground|space|joint|support))\Z')
HASH = re.compile(r'[a-f0-9]{64}\Z')


def _digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _hash(value):
    if not isinstance(value, str) or not HASH.fullmatch(value):
        raise ValueError('Expected an immutable SHA256 binding')
    return value


def _text(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ValueError('Missing native identity or decision identity')
    return value


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _table(value):
    parts = value.split('.') if isinstance(value, str) else []
    if not 1 <= len(parts) <= 2 or any(not IDENTIFIER.fullmatch(p) for p in parts):
        raise ValueError('Invalid immutable candidate relation')
    return '.'.join(parts)


def extent_digest(slices):
    """Hash the exact resolved allocation extent, independently of dollar values."""
    fields = (*KEY, 'scope_sha256')
    rows = {tuple(row[field] for field in fields) for row in slices}
    return _digest(sorted(rows, key=lambda row: tuple(str(value) for value in row)))


@dataclass(frozen=True)
class Binding:
    main_sha256: str
    ledger_sha256: str
    registry_sha256: str
    dictionary_sha256: str

    def __post_init__(self):
        for value in asdict(self).values():
            _hash(value)

    @property
    def sha256(self):
        return _digest(asdict(self))


def project_memberships(binding, active_decisions, resolved_extents, held_slices=()):
    """Produce a money-free bridge from active approvals and exact ETL extents.

    No amount predicate is used. A changed value cannot revoke a classification.
    Whole-contract inheritance/new actions are resolved by the existing canonical
    binder, INCLUDING its exceptions, before this function. Each extent must name
    one existing allocation slice; there is deliberately no whole-native wildcard.
    Holds are full KEY tuples and suppress new associations, not existing finance.
    """
    resolved_extents = list(resolved_extents)
    decisions = {}
    for item in active_decisions:
        ident = _text(item['decision_id'])
        if ident in decisions:
            raise ValueError('More than one active revision for a canonical decision')
        if type(item['decision_revision']) is not int or item['decision_revision'] < 1:
            raise ValueError('Invalid decision revision')
        if item['review_status'] != 'APPROVED' or item['registry_sha256'] != binding.registry_sha256:
            raise ValueError('Decision is not approved at the selected registry checkpoint')
        if (item['target_dimension'] not in ('market', 'platform')
                or not isinstance(item['target_id'], str) or not MARKET.fullmatch(item['target_id'])):
            raise ValueError('Invalid reviewed market target')
        if item['relationship'] not in ('dedicated', 'shared'):
            raise ValueError('Unsupported market relationship')
        for field in ('extent_sha256', 'evidence_sha256'):
            _hash(item[field])
        decisions[ident] = dict(item)
    for ident, decision in decisions.items():
        extent = [row for row in resolved_extents if row['decision_id'] == ident]
        if not extent or extent_digest(extent) != decision['extent_sha256']:
            raise ValueError('Resolved extent differs from the active approved allocation extent')
    held = {tuple(key) for key in held_slices}
    if any(len(key) != len(KEY) for key in held):
        raise ValueError('Hold must bind the complete native financial slice')
    rows = {}
    for extent in resolved_extents:
        if extent['binding_sha256'] != binding.sha256:
            raise ValueError('Extent belongs to a different Main/ledger/registry/dictionary')
        decision = decisions.get(extent['decision_id'])
        if decision is None or any(extent[field] != decision[field] for field in
                ('decision_revision', 'extent_sha256')):
            raise ValueError('Extent does not belong to the active decision revision')
        key = tuple(extent[field] for field in KEY)
        if key[0] not in SOURCES or type(key[2]) is not int or not 2019 <= key[2] <= 2100:
            raise ValueError('Invalid native source/year')
        for value in (key[1], key[3], key[4]):
            _text(value)
        _hash(extent['scope_sha256'])
        if key in held:
            continue
        row = {field: extent[field] for field in (*KEY, 'scope_sha256')}
        row.update({field: decision[field] for field in ('target_dimension', 'target_id', 'relationship',
            'decision_id', 'decision_revision', 'extent_sha256', 'evidence_sha256')})
        row['binding_sha256'] = binding.sha256
        identity = tuple(row[field] for field in sorted(MEMBERSHIP_COLUMNS))
        rows[identity] = row
    return sorted(rows.values(), key=lambda row: tuple(str(row[f]) for f in sorted(row)))


@dataclass(frozen=True)
class NativeMarketSelection:
    binding: Binding
    facts: str
    memberships: str
    fact_columns: frozenset
    membership_columns: frozenset

    def __post_init__(self):
        _table(self.facts)
        _table(self.memberships)
        # Copy containers so caller mutation cannot change the accepted schema.
        object.__setattr__(self, 'fact_columns', frozenset(self.fact_columns))
        object.__setattr__(self, 'membership_columns', frozenset(self.membership_columns))
        if not FACT_COLUMNS <= self.fact_columns:
            raise ValueError('Market selection requires native allocation grain, not rolled-up awards')
        if not MEMBERSHIP_COLUMNS <= self.membership_columns:
            raise ValueError('Incomplete approved membership projection')

    def validation_sql(self):
        """Immutable build gate: must return zero issues before publication.

        These checks do not certify upstream source/DLA eligibility or approval
        semantics. Those existing release gates and the projection builder remain
        mandatory. Facts must already be the eligible canonical financial ledger.
        Readers reuse the pinned successful build receipt; never run a full-ledger
        validation scan for every page request.
        """
        missing = ' OR '.join(f'{key} IS NULL' for key in (*KEY, 'scope_sha256', 'market_node', 'net_usd'))
        blanks = ' OR '.join(f"trim({key})=''" for key in KEY if key != 'fiscal_year')
        keys = ','.join(KEY)
        join = ' AND '.join(f'f.{key}=m.{key}' for key in KEY)
        return f"""WITH issues AS (
          SELECT 'INCOMPLETE_FINANCIAL_SLICE' reason FROM {self.facts}
            WHERE {missing} OR {blanks}
          UNION ALL SELECT 'DUPLICATE_FINANCIAL_SLICE' FROM {self.facts}
            GROUP BY {keys} HAVING count(*)<>1
          UNION ALL SELECT 'STALE_MEMBERSHIP_BINDING' FROM {self.memberships}
            WHERE binding_sha256 IS NULL OR binding_sha256<>{_literal(self.binding.sha256)}
          UNION ALL SELECT 'INCOMPLETE_MEMBERSHIP' FROM {self.memberships}
            WHERE target_dimension IS NULL OR target_dimension NOT IN ('market','platform')
              OR target_id IS NULL OR trim(target_id)=''
              OR relationship IS NULL OR relationship NOT IN ('dedicated','shared')
              OR decision_id IS NULL OR trim(decision_id)='' OR decision_revision IS NULL OR decision_revision<1
              OR extent_sha256 IS NULL OR length(extent_sha256)<>64
              OR evidence_sha256 IS NULL OR length(evidence_sha256)<>64
          UNION ALL SELECT 'ORPHAN_OR_CHANGED_SCOPE' FROM {self.memberships} m
            WHERE NOT EXISTS (SELECT 1 FROM {self.facts} f WHERE {join}
              AND f.scope_sha256=m.scope_sha256)
        ) SELECT reason,count(*) issues FROM issues GROUP BY reason"""

    def selected_sql(self, markets, years, sources, *, include_shared=True, activity=True, platforms=()):
        """Union existing primary and reviewed related memberships BEFORE rollup.

        `markets` is the requested union after the existing typed taxonomy resolves
        descendants. Family dedication needs its own reviewed family membership;
        it is never inferred from shared leaf platform tags.
        """
        if type(include_shared) is not bool or type(activity) is not bool:
            raise ValueError('Shared/activity policy must be explicit booleans')
        # A scalar such as 'US' otherwise becomes the targets 'U' and 'S'.
        # Copy accepted containers before validation and SQL generation.
        filters = (markets, years, sources, platforms)
        if any(not isinstance(values, (list, tuple, set, frozenset)) for values in filters):
            raise ValueError('Market, year, source and platform filters must be collections')
        markets, years, sources, platforms = (tuple(values) for values in filters)
        if (not markets and not platforms) or len(markets) + len(platforms) > 512 or any(
                not isinstance(n, str) or not MARKET.fullmatch(n) for n in (*markets, *platforms)):
            raise ValueError('Invalid market union')
        if not years or len(years) > 82 or any(type(y) is not int or not 2019 <= y <= 2100 for y in years):
            raise ValueError('Invalid fiscal years')
        if not sources or any(s not in SOURCES for s in sources):
            raise ValueError('Invalid source measures')
        nodes = ','.join(_literal(n) for n in sorted(set(markets)))
        fiscal = ','.join(str(y) for y in sorted(set(years)))
        kinds = ','.join(_literal(s) for s in sorted(set(sources)))
        primary = f'f.market_node IN ({nodes})' if markets else '1=0'
        targets = []
        if markets:
            targets.append(f"(m.target_dimension='market' AND m.target_id IN ({nodes}))")
        if platforms:
            platform_ids = ','.join(_literal(n) for n in sorted(set(platforms)))
            targets.append(f"(m.target_dimension='platform' AND m.target_id IN ({platform_ids}))")
        target_filter = '(' + ' OR '.join(targets) + ')'
        join = ' AND '.join(f'm.{key}=f.{key}' for key in KEY)
        membership = (f'SELECT 1 FROM {self.memberships} m WHERE {join}'
            f' AND m.scope_sha256=f.scope_sha256 AND m.binding_sha256={_literal(self.binding.sha256)}'
            f' AND {target_filter}')
        dedicated = f"EXISTS ({membership} AND m.relationship='dedicated')"
        shared = f"EXISTS ({membership} AND m.relationship='shared')"
        selected = primary
        if activity:
            selected += f' OR {dedicated}' + (f' OR {shared}' if include_shared else '')
        bucket = (f"CASE WHEN {primary} THEN 'primary_allocation' "
            f"WHEN {dedicated} THEN 'related_dedicated' ELSE 'shared_exposure' END") if activity else "'primary_allocation'"
        # f.* preserves existing native details. EXISTS cannot multiply an amount.
        # Amounts never appear in the bridge, and SUM(DISTINCT amount) is forbidden.
        return f"""SELECT f.*,{bucket} AS activity_bucket FROM {self.facts} f
          WHERE f.fiscal_year IN ({fiscal}) AND f.source_kind IN ({kinds})
            AND ({selected})"""

    def fiscal_sql(self, *args, **kwargs):
        selected = self.selected_sql(*args, **kwargs)
        # Keep source measures separate; first-tier values are not prime size.
        return f"""SELECT source_kind,fiscal_year,activity_bucket,
          count(*) financial_slices,count(DISTINCT source_record_id) awards,
          sum(net_usd) signed_usd FROM ({selected}) selected
          GROUP BY source_kind,fiscal_year,activity_bucket"""
