import json
import math
import os
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
CITY_ID = os.getenv("CITY_ID", "logrono")
SENSOR_COUNT = int(os.getenv("SENSOR_COUNT", "100"))
INTERVAL = float(os.getenv("INTERVAL_SECONDS", "1"))

DISTRICTS = [
    ("Centro", "CITY_CENTER", 42.4658, -2.4498, 4.0),
    ("Parque del Carmen", "PARK", 42.4639, -2.4472, -1.5),
    ("Varea", "INDUSTRIAL", 42.4477, -2.4034, 3.0),
    ("San Adrián", "RESIDENTIAL", 42.4567, -2.4661, 1.0),
    ("Las Gaunas", "TRANSPORT", 42.4541, -2.4490, 2.0),
]

def build_sensors():
    sensors = []
    for i in range(1, SENSOR_COUNT + 1):
        district, zone, lat, lon, heat_bias = DISTRICTS[(i - 1) % len(DISTRICTS)]
        sensors.append({
            "device_id": f"LOG-{i:04d}",
            "district": district,
            "zone_type": zone,
            "latitude": lat + random.uniform(-0.002, 0.002),
            "longitude": lon + random.uniform(-0.002, 0.002),
            "heat_bias": heat_bias,
            "base": random.uniform(22, 25),
            "battery": random.uniform(65, 100),
        })
    return sensors

def generate_measurement(sensor):
    now = datetime.now(timezone.utc)
    hour = now.hour + now.minute / 60

    # Ciclo térmico diario: mínimo de madrugada, máximo por la tarde.
    daily_cycle = 7 * math.sin((hour - 8) / 24 * 2 * math.pi)
    noise = random.gauss(0, 0.35)
    temperature = sensor["base"] + daily_cycle + sensor["heat_bias"] + noise

    humidity = max(15, min(95, 65 - (temperature - 20) * 1.5 + random.gauss(0, 2)))
    surface_temperature = temperature + random.uniform(5, 13)
    solar = max(0, 900 * math.sin((hour - 6) / 12 * math.pi) + random.gauss(0, 30))

    sensor["battery"] = max(0, sensor["battery"] - random.uniform(0.0001, 0.001))

    # Una pequeña probabilidad de anomalía para probar el pipeline.
    if random.random() < 0.0005:
        temperature += random.choice([-25, 25])

    return {
        "device_id": sensor["device_id"],
        "city_id": CITY_ID,
        "timestamp": now.isoformat(),
        "location": {
            "latitude": sensor["latitude"],
            "longitude": sensor["longitude"],
            "district": sensor["district"],
            "zone_type": sensor["zone_type"],
        },
        "temperature": round(temperature, 2),
        "humidity": round(humidity, 2),
        "surface_temperature": round(surface_temperature, 2),
        "solar_radiation": round(solar, 2),
        "battery_level": round(sensor["battery"], 2),
        "signal_strength": random.randint(-70, -35),
        "firmware_version": "1.4.2",
    }

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="urbanheat-simulator")
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_start()

    sensors = build_sensors()
    print(f"Simulando {len(sensors)} sensores en {CITY_ID}")

    try:
        while True:
            for sensor in sensors:
                payload = generate_measurement(sensor)
                topic = f"city/{CITY_ID}/sensors/{sensor['device_id']}/measurements"
                client.publish(topic, json.dumps(payload), qos=0)
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
