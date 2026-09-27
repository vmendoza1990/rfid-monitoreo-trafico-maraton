import json
import time
import random
import uuid
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

CHECKPOINT_PARTITIONS = {
    "KM-10": 0,
    "KM-21": 1,
    "META": 2,
}

def create_event(checkpoint, runner_id, delay_minutes=0):
    """Crea un evento RFID."""
    # Resetea los minutos de retraso para simular un evento tardío (lateness)
    event_time = datetime.now(timezone.utc) - timedelta(minutes=delay_minutes)
    
    return {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),  # Para deduplicar
        "key": checkpoint,              # Clave de partición
        "event_time": event_time.isoformat().replace("+00:00", "Z"),
        "payload": {"runner_id": runner_id}
    }

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',
        retries=3
    )
    
    checkpoints = ["KM-10", "KM-21", "META"]
    
    print("Iniciando la carrera... inyecar lecturas RFID a Kafka (tópico: rfid.reads)")
    
    try:
        while True:
            # 1. Evento Normal
            runner = f"RUNNER-{random.randint(1000, 9999)}"
            checkpoint = random.choice(checkpoints)
            event = create_event(checkpoint, runner)
            
            #producer.send('rfid.reads', key=event["key"], value=event)
            producer.send('rfid.reads', key=event["key"], value=event, partition=CHECKPOINT_PARTITIONS[event["key"]])
            print(f"[NORMAL] Corredor {runner} pasó por {checkpoint}")
            
            # 2. Simulación de Duplicado (15% de probabilidad)
            if random.random() < 0.15:
                # Enviar exactamente el mismo evento (mismo event_id) para probar la deduplicación
                #producer.send('rfid.reads', key=event["key"], value=event)
                producer.send('rfid.reads', key=event["key"], value=event, partition=CHECKPOINT_PARTITIONS[event["key"]])
                print(f"  -> [DUPLICADO] Doble lectura del corredor {runner}")
                
            # 3. Simulación de Evento Tardío / Lateness (10% de probabilidad)
            if random.random() < 0.10:
                late_runner = f"RUNNER-{random.randint(1000, 9999)}"
                # Evento generado con 3 minutos de retraso
                late_event = create_event("KM-21", late_runner, delay_minutes=3)
                #producer.send('rfid.reads', key=late_event["key"], value=late_event)
                producer.send('rfid.reads', key=late_event["key"], value=late_event, partition=CHECKPOINT_PARTITIONS[late_event["key"]])
                print(f"  -> [TARDÍO] Lectura retrasada del corredor {late_runner} en KM-21")
                
            time.sleep(1.5)
            
    except KeyboardInterrupt:
        print("Carrera finalizada.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_producer()