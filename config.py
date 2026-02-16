import json
from pathlib import Path

DEFAULT_CONFIG = {
    "paths": {
        "raw_data": "data/raw",
        "processed_data": "data/processed"
    },
    "universe": {
        "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
    },
    "data": {
        "price": {
            "start_date": "2015-01-01",
            "end_date": "2026-01-01"
        },
        "fundamentals": {
            "api_key": "438de359-d321-4ce9-8333-7cdc02afd711",
            "mapping_file": "data/simfin_mapping.csv"
        },
        "macro": {
            "fred_api_key": ""
        }
    }
}

def load_config(path="config.json"):
    path = Path(path)
    if not path.exists():
        path.write_text(json.dumps(DEFAULT_CONFIG, indent=2))
        return DEFAULT_CONFIG
    return json.loads(path.read_text())
