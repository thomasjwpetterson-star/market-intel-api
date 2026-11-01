"""Shared reference-value guard for serving and whole-market coverage totals."""


def reference_request_value_sql(extract='json_extract_scalar'):
    if extract not in ('json_extract_scalar','json_extract_string'):
        raise ValueError('Unsupported JSON scalar function')
    def field(name):return extract+"(o.value_inputs_json,'$."+name+"')"
    quantity='try_cast('+field('quantity')+' AS DOUBLE)'
    price='try_cast('+field('observed_unit_price')+' AS DOUBLE)'
    maximum='try_cast('+field('source_contract_maximum_usd')+' AS DOUBLE)'
    amount=quantity+'*'+price
    return "CASE WHEN o.lane='published_item_procurement' AND "+quantity+">0 AND "+price+">0 AND nullif("+field('request_unit')+",'') IS NOT NULL AND coalesce("+field('pricing_conflict')+",'false')<>'true' AND (coalesce("+maximum+",0)<=0 OR "+amount+'<='+maximum+') THEN '+amount+' END'
