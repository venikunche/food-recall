from curl_cffi import requests

# What we currently get (default — probably only active recalls)
response = requests.get(
    "https://www.fsis.usda.gov/fsis/api/recall/v/1",
    impersonate="chrome"
)
data_default = response.json()
print(f"Default (no params): {len(data_default)} records")

# Now try with field_archive_recall=All to get everything
response_all = requests.get(
    "https://www.fsis.usda.gov/fsis/api/recall/v/1",
    params={"field_archive_recall": "All"},
    impersonate="chrome"
)
data_all = response_all.json()
print(f"With archive=All: {len(data_all)} records")

# Check year distribution of the full dataset
from collections import Counter
years = []
for r in data_all:
    date = r.get("field_recall_date", "")
    if date and len(date) >= 4:
        years.append(date[:4])
for year, count in sorted(Counter(years).items(), reverse=True):
    print(f"  {year}: {count}")
