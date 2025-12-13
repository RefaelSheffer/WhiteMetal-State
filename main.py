import json
from datetime import datetime

signal = {
    "timestamp": datetime.utcnow().isoformat(),
    "market_state": "NEUTRAL",
    "action": "HOLD",
    "confidence": 0.50,
    "explanation": "Initial placeholder signal. No live data yet."
}

output_path = "public/data/latest_signal.json"

with open(output_path, "w") as f:
    json.dump(signal, f, indent=2)

print(f"Signal written to {output_path}")
