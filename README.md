# UrbanHeat Platform

Plataforma Big Data de referencia para recibir datos IoT de sensores ambientales urbanos, procesarlos en streaming y generar analítica sobre temperatura e islas de calor.

## Arquitectura

```text
IoT Simulator
     |
     | MQTT
     v
Eclipse Mosquitto
     |
     | bridge
     v
Apache Kafka
     |
     +--------------------+
     |                    |
     v                    v
Spark Structured      Raw topic
Streaming                 |
     |                    v
     |                  MinIO
     |
     +--> PostgreSQL
     |
     +--> Kafka alerts
              |
              v
           Grafana
```

## Stack

- Python 3.12
- MQTT / Eclipse Mosquitto
- Apache Kafka
- PySpark / Spark Structured Streaming
- MinIO (Data Lake S3 compatible)
- PostgreSQL
- Grafana
- Docker Compose

## Inicio rápido

Requisitos:

- Docker Desktop
- Docker Compose v2
- 8 GB RAM recomendados

Arranque:

```powershell
docker compose up -d --build
```

Comprobar:

```powershell
docker compose ps
```

Interfaces:

- Grafana: http://localhost:3000
- Kafka UI: http://localhost:8080
- MinIO: http://localhost:9001
- PostgreSQL: localhost:5432
- MQTT: localhost:1883

Grafana:

```text
usuario: admin
contraseña: admin
```

## Ejecutar el simulador

El simulador se arranca automáticamente con Docker Compose.

Para ejecutarlo manualmente:

```powershell
docker compose run --rm iot-simulator
```

Variables útiles:

```text
SENSOR_COUNT=100
INTERVAL_SECONDS=1
CITY_ID=logrono
```

El simulador representa 100 sensores enviando una medición por segundo. Para una demo de 1.000 sensores se puede aumentar `SENSOR_COUNT`.

## Flujo de datos

1. El simulador genera temperatura, humedad, temperatura superficial, radiación, batería y señal.
2. Publica por MQTT.
3. `mqtt-kafka-bridge` consume MQTT y publica en Kafka.
4. Spark consume `iot.raw.measurements`.
5. Spark valida y limpia.
6. Los datos se escriben en MinIO en capas Bronze/Silver/Gold.
7. Spark actualiza PostgreSQL con métricas agregadas.
8. Las condiciones críticas generan eventos en `iot.heat.alerts`.
9. Grafana consulta PostgreSQL.

## Modelo Medallion

```text
bronze/
  measurements/year=YYYY/month=MM/day=DD/hour=HH/

silver/
  measurements/year=YYYY/month=MM/day=DD/hour=HH/

gold/
  district_hourly/year=YYYY/month=MM/day=DD/
  heat_index/year=YYYY/month=MM/day=DD/
```

## Reglas

### Validación

- Temperatura: -40 a 70 °C
- Humedad: 0 a 100 %
- Batería: 0 a 100 %
- Radiación: 0 a 1500 W/m²

### Anomalías

- Valor físicamente imposible.
- Cambio de temperatura > 10 °C respecto de la lectura anterior.
- Sensor con demasiadas lecturas idénticas consecutivas.

### Índice UHI

```text
UHI = temperatura_media_zona_urbana - temperatura_media_parques
```

Clasificación:

- < 2 °C: LOW
- 2–4 °C: MODERATE
- 4–6 °C: HIGH
- > 6 °C: CRITICAL

## Desarrollo por fases

### Fase 1
MQTT -> PostgreSQL -> Grafana.

### Fase 2
MQTT -> Kafka.

### Fase 3
Kafka -> Spark -> MinIO.

### Fase 4
Agregaciones, anomalías y alertas.

### Fase 5
Escalado del simulador y métricas de rendimiento.

## Estructura

```text
urbanheat-platform/
├── docker-compose.yml
├── .env.example
├── README.md
├── iot-simulator/
├── mqtt-kafka-bridge/
├── spark/
├── database/
├── mosquitto/
├── grafana/
└── scripts/
```
