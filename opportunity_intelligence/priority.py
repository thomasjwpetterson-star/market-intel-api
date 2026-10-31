"""Dated supply context adjusts forecast priority, never the demand value."""
import datetime
import hashlib
from pathlib import Path

POLICY = 'dated-stock-forecast-priority-v1'
POLICY_HASH = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def inventory_priority(columns, as_of, forecast_value, unit_price):
    datetime.date.fromisoformat(as_of)
    if not {'inventory_date', 'stock_on_hand', 'backorder_quantity'}.issubset(columns):
        return '0.0', "'Comparable dated stock evidence is unavailable'"
    fresh = "try_cast(substr(cast(v.inventory_date AS varchar),1,10) AS date) BETWEEN date_add('day',-90,DATE '" + as_of + "') AND DATE '" + as_of + "'"
    conditions = [(fresh + ' AND v.backorder_quantity>0', 10, 'Backorders were recorded in the dated stock snapshot; current orders and stock allocations remain unconfirmed')]
    if {'reorder_date', 'reorder_stock', 'reorder_point'}.issubset(columns):
        reorder_fresh = "try_cast(substr(cast(v.reorder_date AS varchar),1,10) AS date) BETWEEN date_add('day',-90,DATE '" + as_of + "') AND DATE '" + as_of + "'"
        conditions.append((reorder_fresh + ' AND v.reorder_stock>=0 AND v.reorder_stock<v.reorder_point', 6, 'The dated stock snapshot was below its stated reorder point; a new purchasing requirement is not confirmed'))
    conditions.append((fresh + ' AND v.stock_on_hand>=0 AND (' + forecast_value + ')>0 AND (' + unit_price + ')>0 AND v.stock_on_hand*(' + unit_price + ')>=(' + forecast_value + ')', -8, 'Stock at the dated snapshot covered the next twelve-month forecast quantity; retain as a longer-term demand watch, not a confirmed new purchase'))
    points = "CASE WHEN o.lane<>'source_item_forecasts' THEN 0.0 " + ''.join('WHEN ' + condition + ' THEN ' + str(value) + '.0 ' for condition, value, _ in conditions) + 'ELSE 0.0 END'
    basis = "CASE WHEN o.lane<>'source_item_forecasts' THEN 'Stock context does not change this procurement priority' " + ''.join("WHEN " + condition + " THEN '" + reason + "' " for condition, _, reason in conditions) + "ELSE 'No dated replenishment-pressure or stock-coverage adjustment; forecast demand does not establish net new purchasing' END"
    return points, basis
