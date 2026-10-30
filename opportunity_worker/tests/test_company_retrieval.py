from opportunity_engine.company_enrichment import scope_terms,admissible,nsns
from opportunity_engine import rules

def test_functional_scope_does_not_use_generic_hardware_as_a_capability():
 assert scope_terms('AIRCRAFT COMPONENT ASSEMBLY')==[]
 assert scope_terms('Search, Detection, Navigation, Guidance, Aeronautical and Nautical System and Instrument Manufacturing')==[]
 assert scope_terms('FUEL BOOST PUMP')==['boost','fuel','pump']

def test_similar_part_is_not_an_accessible_procurement():
 o={'title':'Fuel boost pump','scope_text':'NSN: 1680-01-531-2588SO Part Number: 62218-1','lane':'published_procurement'}
 assert not admissible('FUEL BOOST PUMP',o,'published_requirement')
 assert admissible('FUEL BOOST PUMP',o,'qualification_opening')
 assert nsns(o['scope_text'])==['1680015312588']

def test_authoritative_sources_sought_type_is_not_lost_when_description_omits_label():
 o={'lane':'published_procurement','title':'Repair of aircraft fuel boost pump','scope_text':'The Government is conducting market research to identify potential sources that possess expertise to meet qualification requirements.','customer_name':'Air Force / FA8524','source_evidence_id':'native-source','record_hash':'hash','notice_type':'Sources Sought','notice_posted_date':'2026-09-24','window_end':None}
 d=rules.evaluate(o,'2026-10-30')
 assert d['stage']=='qualification_opening'
 assert d['response_window_verified'] is False
 assert 'confirm' in d['next_action'].lower()
 o['window_end']='2026-10-01'
 assert rules.evaluate(o,'2026-10-30')['stage']=='resolved_or_elapsed'

def test_access_check_rejects_generic_supply_pull_and_incompatible_work_modes():
 from opportunity_engine.company_enrichment import scope_terms,admissible
 assert not scope_terms('MAJOR SYS/SUBSYS')
 assert not scope_terms('INSTRUMENT MANUFACTURING FOR MEASURING AND TESTING')
 assert not admissible('JUNCTION BOX',{'title':'New prime junction box housing assembly','scope_text':'Junction box housing assembly','lane':'new_prime_historical_supplier_flow'},'emerging_demand')
 assert not admissible('REPAIR OF RECEIVER-TRANSMITTER',{'title':'Receiver transmitter production','scope_text':'Manufacture receiver transmitters','lane':'new_prime_historical_supplier_flow'},'emerging_demand')
 assert not admissible('F-16 radar overhaul',{'title':'F-35 radar overhaul','scope_text':'Seeking sources to overhaul F-35 radar','lane':'published_procurement'},'qualification_opening')
