# test_quick.py (temporary, at project root)
import json
from src.extractors.fda_extractor import FDAExtractor

extractor = FDAExtractor()

# Just grab the first page to verify it works
data = extractor._fetch_page(skip=0)
print(f"Total records available: {data['meta']['results']['total']}")
print(f"First record keys: {list(data['results'][0].keys())}")
print(json.dumps(data['results'][0], indent=2))
