# test_quick_fsis.py (temporary, at project root)
import json
from src.extractors.fsis_extractor import FSISExtractor

extractor = FSISExtractor()
records = extractor.extract_all()

print(f"Total FSIS records: {len(records)}")
print(f"Fields in first record: {list(records[0].keys())}")
print(json.dumps(records[0], indent=2))
