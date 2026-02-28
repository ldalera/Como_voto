import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent.parent
idx_path = base / 'docs' / 'data' / 'legislators.json'
leg_dir = base / 'docs' / 'data' / 'legislators'

if not idx_path.exists():
    print('Index file not found:', idx_path)
    raise SystemExit(1)

with open(idx_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

missing = []
for entry in data:
    k = entry.get('k','')
    safe = re.sub(r'[^A-Z0-9_]', '_', k)[:80]
    path = leg_dir / (safe + '.json')
    if not path.exists():
        missing.append((k, safe, str(path)))

if not missing:
    print('All legislator files present for', len(data), 'entries')
else:
    print('Missing files:', len(missing))
    for i, (k, safe, p) in enumerate(missing[:50], start=1):
        print(i, k, '=>', safe, '->', p)
    if len(missing) > 50:
        print('... and', len(missing)-50, 'more')
