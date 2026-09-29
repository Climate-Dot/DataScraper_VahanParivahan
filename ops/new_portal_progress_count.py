"""Checkpoint progress for one year. Set YEAR to override the default."""
import os
from pathlib import Path

from new_portal.rto_fetch import build_progress_path, load_progress, office_is_complete

year = int(os.environ.get("YEAR") or 2026)
path = build_progress_path(year)
p = load_progress(path)
complete = sum(1 for v in p.values() if office_is_complete(v))
rows = sum(len(v[0]) for v in p.values())
gaps = sum(len(v[1]) for v in p.values())
print(f"recorded {len(p)}/1676 | complete {complete} | gapped {len(p)-complete} "
      f"| rows {rows} | gaps {gaps}")
