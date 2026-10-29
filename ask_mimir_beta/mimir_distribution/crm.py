"""Selected-record CSV demonstration. Does not contact or write to any CRM."""
import csv
import io
import json
from .models import Opportunity

FIELDS = list(Opportunity.model_fields)

def export_selected(records: list[Opportunity]) -> str:
    if not 1 <= len(records) <= 50:
        raise ValueError("Select between 1 and 50 opportunity records")
    if len({r.external_id for r in records}) != len(records):
        raise ValueError("Duplicate external opportunity ID")
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=FIELDS)
    writer.writeheader()
    for record in records:
        row = record.model_dump(mode="json")
        for key, value in row.items():
            value = json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else "" if value is None else str(value)
            # Protect Excel/CSV consumers against formula execution, including leading whitespace.
            row[key] = "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) or value.startswith(("\t", "\r", "\n")) else value
        writer.writerow(row)
    return output.getvalue()
