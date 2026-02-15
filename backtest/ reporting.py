import pandas as pd
from pathlib import Path
from datetime import datetime

def append_report(report_path, data: dict):
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    data = {"timestamp": datetime.now().isoformat(), **data}

    if report_path.exists():
        df = pd.read_csv(report_path)
        df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
    else:
        df = pd.DataFrame([data])

    df.to_csv(report_path, index=False)
