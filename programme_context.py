"""Small sourced context notes; programme amounts always come from live records."""
import re
from ask_mimir_beta.platform_description_mapping import DEFAULT_RULES

AMRAAM = {
    'programme': 'AMRAAM',
    'full_name': 'Advanced Medium-Range Air-to-Air Missile',
    'facts': [
        'AMRAAM is a joint Air Force and Navy programme, with the Air Force as lead service.',
        'NAVAIR lists both Air Force and Navy/Marine Corps aircraft as AMRAAM users.',
    ],
    'analytical_implications': [
        'A recorded contracting agency identifies the buying channel; it does not identify every funding service or end user.',
        'Air Force-led joint procurement is a reason to examine programme-wide contracts alongside a Navy-only agency filter.',
    ],
    'source': {'title': 'NAVAIR — AMRAAM', 'url': 'https://www.navair.navy.mil/product/AMRAAM', 'checked_on': '2026-11-04'},
}


def normalized(value):
    return ' '.join(re.findall(r'[A-Z0-9]+', str(value).upper()))


def context_for(programme):
    return AMRAAM if normalized(programme) in {'AMRAAM', 'AIM 120', 'AIM120'} else None


def aliases_for(programme):
    key = normalized(programme)
    for rule in DEFAULT_RULES:
        if key in {normalized(rule.display_name), normalized(rule.platform_id), *(normalized(a) for a in rule.aliases)}:
            aliases = list(rule.aliases)
            break
    else:
        aliases = [programme]
    if context_for(programme):
        aliases += ['AIM-120', 'AIM 120', 'AIM120']
    return list(dict.fromkeys(a.strip().upper() for a in aliases))[:8]
