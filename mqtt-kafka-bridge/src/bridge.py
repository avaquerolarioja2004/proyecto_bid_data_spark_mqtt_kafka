import os
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw.measurements")

producer = None

def connect_kafka():
    global producer
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA,
                value_serializer=lambda v: v.encode("utf-8"),
                acks="all",
                retries=5,
            )
        except Exception as exc:
            print(f"Kafka todavía no disponible: {exc}")
            time.sleep(3)

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"MQTT conectado: {reason_code}")
    client.subscribe("city/+/sensors/+/measurements", qos=0)

def on_message(client, userdata, msg):
    try:
        producer.send(TOPIC, value=msg.payload.decode("utf-8"))
    except Exception as exc:
        print(f"Error enviando a Kafka: {exc}")

def main():
    connect_kafka()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-kafka-bridge")
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as exc:
            print(f"MQTT no disponible: {exc}")
            time.sleep(3)

if __name__ == "__main__":
    main()
