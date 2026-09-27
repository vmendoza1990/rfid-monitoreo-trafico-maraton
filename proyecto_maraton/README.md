# Monitor de Tráfico RFID para Maratones (Streaming End-to-End)

Pipeline de streaming end-to-end con **Apache Kafka** y **Apache Beam** para monitorear en tiempo real el tráfico de corredores en una maratón, a partir de lecturas de chips RFID en puntos de control (checkpoints).

Proyecto integrador — Curso *Streaming de datos y sus aplicaciones*, Maestría en Inteligencia Artificial, FPUNA.

**Equipo:** Víctor Mendoza, Luis Ríos

📄 Documentación técnica completa: [`docs/documento_tecnico.md`](docs/documento_tecnico.md)

---

## ¿Qué hace este proyecto?

Simula lecturas RFID de corredores pasando por tres puntos de control (`KM-10`, `KM-21`, `META`), incluyendo escenarios adversos realistas de un evento al aire libre: **lecturas duplicadas** (reenvíos de red) y **lecturas tardías/fuera de orden**. Un pipeline de Apache Beam consume esos eventos desde Kafka, los deduplica, los agrupa en ventanas de tiempo de 60 segundos usando **tiempo de evento** (no tiempo de llegada), publica conteos de corredores por checkpoint de forma **idempotente**, y al finalizar la operación entrega un **resumen final de atletas únicos por checkpoint**.

## Arquitectura (resumen)

```
Productor sintético → Kafka (rfid.reads, 3 particiones)
                          → Apache Beam (validación, ventanas, dedup, conteo incremental)
                              → Kafka (traffic.alerts, 3 particiones) + consola
                              → Resumen final por checkpoint (al detener el pipeline)
```

Diagrama completo y detalle de cada decisión de diseño en el [documento técnico](docs/documento_tecnico.md).

## Estructura del repositorio

```
proyecto_maraton/
├── src/
│   ├── pipeline.py      # Pipeline de Apache Beam (consumo, procesamiento, salida, resumen)
│   └── producer.py      # Productor sintético de eventos RFID
├── test/
│   └── test_pipeline.py # Pruebas: TestPipeline (dedup) y TestStream (ventanas/lateness)
├── docs/
│   ├── documento_tecnico.md
│   └── evidencias/       # Capturas de la demostración end-to-end
├── docker-compose.yml     # Kafka (modo KRaft) + creación automática de tópicos
├── requirements.txt
└── README.md
```

## Prerrequisitos

- Python 3.11
- Docker y Docker Compose
- Puerto `9092` disponible en el host

## Instalación

```bash
# Clonar el repositorio
git clone <url-del-repositorio>
cd proyecto_maraton

# Instalar dependencias de Python
pip install -r requirements.txt --break-system-packages
```

## Ejecución

### 1. Levantar la infraestructura de Kafka

```bash
docker compose up -d
```

Esto levanta un broker Kafka en modo KRaft (sin ZooKeeper) y crea automáticamente los tópicos `rfid.reads` y `traffic.alerts`, ambos con 3 particiones.

Verificar que los tópicos se crearon correctamente:

```bash
docker exec -it proyecto_maraton-kafka-1 /opt/kafka/bin/kafka-topics.sh --describe --topic rfid.reads --bootstrap-server 127.0.0.1:9092
```

Debería mostrar `PartitionCount: 3`.

### 2. Ejecutar las pruebas

```bash
python test/test_pipeline.py
```

Corre dos pruebas: deduplicación en memoria (`TestPipeline`) y comportamiento ante eventos tardíos (`TestStream`).

### 3. Demostración end-to-end

Abrir **dos terminales**:

**Terminal A — pipeline (consumidor):**
```bash
python src/pipeline.py
```

**Terminal B — productor:**
```bash
python src/producer.py
```

Dejar correr ambos procesos durante al menos 60-90 segundos. En la Terminal A deberían aparecer bloques como:

```
[ALERTA TRÁFICO - CONFIRMADO EN KAFKA] partition=1 offset=774 -> {'idempotency_key': 'KM-10|2026-09-27T14:46:00', 'checkpoint': 'KM-10', 'window_start': '2026-09-27T14:46:00', 'total_runners': 1}
```

### 4. Finalizar y ver el resumen

1. Presionar `Ctrl+C` primero en la **Terminal B** (productor).
2. Presionar `Ctrl+C` en la **Terminal A** (pipeline) — esto detiene el pipeline de forma ordenada e imprime el resumen final:

```
==================================================
RESUMEN FINAL - ATLETAS ÚNICOS POR CHECKPOINT
==================================================
  KM-10      -> 12 atletas
  KM-21      -> 15 atletas
  META       -> 10 atletas
--------------------------------------------------
  TOTAL      -> 37 pasadas registradas
==================================================

Pipeline detenido correctamente.
```

### 5. Detener y limpiar el entorno

```bash
docker compose down -v
```

## Evidencia de funcionamiento

Ver capturas en [`docs/evidencias/`](docs/evidencias/):
- Productor inyectando eventos normales, duplicados y tardíos.
- Éxito de las pruebas unitarias y de `TestStream`.
- Ejecución en paralelo de productor + pipeline + Kafka.
- Salida idempotente confirmada con partición y offset reales en Kafka.
- Resumen final de atletas por checkpoint al cierre de la operación.
- Verificación de tópicos y particiones vía `kafka-get-offsets.sh`.

## Alcance y limitaciones

Ver sección 10 del [documento técnico](docs/documento_tecnico.md) para mayor detalle.
