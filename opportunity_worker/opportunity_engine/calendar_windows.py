"""Preserve month precision for FCST_DT; real notice dates remain day precise."""
import datetime

def month_start(as_of):
    return datetime.date.fromisoformat(as_of).replace(day=1).isoformat()
