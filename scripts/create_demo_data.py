"""
Herramienta opcional para insertar dispositivos adicionales.
Uso:
    python scripts/create_demo_data.py 100
"""
import json
import sys
from pathlib import Path

count = int(sys.argv[1]) if len(sys.argv) > 1 else 100
districts = [
    ("Centro", "CITY_CENTER"),
    ("Parque del Carmen", "PARK"),
    ("Varea", "INDUSTRIAL"),
    ("San Adrián", "RESIDENTIAL"),
    ("Las Gaunas", "TRANSPORT"),
]

rows = []
for i in range(1, count + 1):
    district, zone = districts[(i - 1) % len(districts)]
    rows.append({
        "device_id": f"LOG-{i:04d}",
        "district": district,
        "zone_type": zone
    })

Path("scripts/sensors-demo.json").write_text(
    json.dumps(rows, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print(f"Generados {count} sensores en scripts/sensors-demo.json")
