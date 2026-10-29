"""Export explicitly selected local demonstration records; no CRM network access."""
import argparse
import json
from pathlib import Path
from .models import Opportunity
from .crm import export_selected

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("selected_json", type=Path)
    parser.add_argument("output_csv", type=Path)
    args = parser.parse_args()
    if args.selected_json.stat().st_size > 512000:
        parser.error("Selected record file exceeds 500 KiB")
    rows = json.loads(args.selected_json.read_text())
    if not isinstance(rows, list) or not 1 <= len(rows) <= 50:
        parser.error("Select between 1 and 50 records")
    args.output_csv.write_text(export_selected([Opportunity.model_validate(row) for row in rows]), encoding="utf-8-sig")

if __name__ == "__main__":
    main()
