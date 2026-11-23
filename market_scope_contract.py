"""Release protocol shared by the publisher and authenticated query compiler."""
from market_scope_membership import KEY, _digest, _hash

DB = 'market_intel_platform_candidate'
SCOPE_POLICY = {'selectionAlgorithm':'native-slice-exists-v1','financialKey':list(KEY),
    'actionKey':list(KEY[:-1]),'activityModes':['primary','activity'],'sharedPolicies':[False,True],
    'sourceMeasures':'separate','primaryAllocation':'unchanged'}


def validate_output_binding(descriptor, legacy_table):
    outputs = descriptor.get('outputInventory')
    expected = {'facts':descriptor['factTable'],'memberships':descriptor['membershipTable'],
                'stock':descriptor['stockTable'],'detail':legacy_table}
    if not isinstance(outputs,dict) or set(outputs) != set(expected):
        raise ValueError('Exact output relation inventory is required')
    for role, table in expected.items():
        entry = outputs[role]
        if (entry.get('database') != DB or entry.get('table') != table
                or not isinstance(entry.get('catalogVersion'),str) or not entry['catalogVersion']):
            raise ValueError('Output relation does not match its catalog version binding')
        _hash(entry.get('schemaSha256'))
        pin=entry.get('objectInventoryPin',{})
        if (not isinstance(pin.get('key'),str) or not pin['key'].startswith('governance/market_intelligence/candidates/')
                or not isinstance(pin.get('version'),str) or not pin['version']):
            raise ValueError('Output object inventory must be versioned')
        _hash(pin.get('sha256'))
    if descriptor.get('outputBindingSha256') != _digest(outputs):
        raise ValueError('Output inventory differs from its immutable binding')
    if descriptor.get('policy') != SCOPE_POLICY or descriptor.get('policySha256') != _digest(SCOPE_POLICY):
        raise ValueError('Market projection policy differs from the approved query protocol')
