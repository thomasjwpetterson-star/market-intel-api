"""Literal, parameterised description searches shared by research endpoints."""
DESCRIPTION_COLUMNS = ('description', 'base_award_description', 'action_description')


def description_terms(value):
    terms = [value] if isinstance(value, str) else value
    if not isinstance(terms, list) or not 1 <= len(terms) <= 8:
        raise ValueError('Use one to eight description terms.')
    clean = []
    for term in terms:
        if not isinstance(term, str) or not 2 <= len(term.strip()) <= 128 or any(ord(c) < 32 for c in term):
            raise ValueError('Use description terms between 2 and 128 characters.')
        if term.strip().upper() not in clean:
            clean.append(term.strip().upper())
    return clean


def description_predicate(terms, columns=DESCRIPTION_COLUMNS):
    selected = [c for c in DESCRIPTION_COLUMNS if c in columns]
    if not selected:
        raise ValueError('Contract descriptions are unavailable in this source.')
    expression = "UPPER(CONCAT_WS(' ', " + ', '.join("COALESCE(CAST("+c+" AS VARCHAR),'')" for c in selected) + '))'
    params = ['%' + t.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%' for t in description_terms(terms)]
    return '(' + ' OR '.join(expression + " LIKE ? ESCAPE '\\'" for _ in params) + ')', params
