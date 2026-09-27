import json
import logging
import signal
import threading
from typing import Any, Tuple
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer, ReadModifyWriteStateSpec
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.coders import StrUtf8Coder, VarIntCoder
from apache_beam.utils.timestamp import Duration
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# ----------------------------------------------------------------------
GLOBAL_LOCK = threading.Lock()
GLOBAL_SEEN_BY_CHECKPOINT = {}   # checkpoint -> set() de event_id únicos vistos en toda la corrida


def registrar_evento_global(element):
    checkpoint, event = element
    with GLOBAL_LOCK:
        GLOBAL_SEEN_BY_CHECKPOINT.setdefault(checkpoint, set()).add(event['event_id'])
    return element


def imprimir_resumen_final():
    print("\n" + "=" * 50)
    print("RESUMEN FINAL - ATLETAS ÚNICOS")
    print("=" * 50)
    with GLOBAL_LOCK:
        if not GLOBAL_SEEN_BY_CHECKPOINT:
            print("No se registraron eventos.")
        else:
            total_general = 0
            for checkpoint in sorted(GLOBAL_SEEN_BY_CHECKPOINT.keys()):
                cantidad = len(GLOBAL_SEEN_BY_CHECKPOINT[checkpoint])
                total_general += cantidad
                print(f"  {checkpoint:<10} -> {cantidad} atletas")
            print("-" * 50)
            print(f"  {'TOTAL':<10} -> {total_general} pasadas registradas")
    print("=" * 50 + "\n")


def _manejar_interrupcion(signum, frame):
    logging.getLogger().setLevel(logging.CRITICAL)
    raise KeyboardInterrupt


class ReadFromKafkaPython(beam.DoFn):
    
    def _create_consumer(self):
        return KafkaConsumer(
            'rfid.reads',
            bootstrap_servers=['127.0.0.1:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='marathon-pipeline-final',
            value_deserializer=lambda v: v,
            key_deserializer=lambda k: k,
        )

    def setup(self):
        self.consumer = self._create_consumer()
        logging.warning("[KAFKA-READER] Consumidor iniciado, esperando mensajes...")

    def process(self, _):
        try:
            records = self.consumer.poll(timeout_ms=800, max_records=500)
        except Exception as e:
            logging.warning(f"[KAFKA-READER] Fallo de socket ({e}). Reiniciando conexión...")
            try:
                self.consumer.close()
            except:
                pass
            self.consumer = self._create_consumer()
            return

        for _tp, messages in records.items():
            for message in messages:
                yield (message.key, message.value)

    def teardown(self):
        if hasattr(self, 'consumer') and self.consumer:
            try:
                self.consumer.close()
            except:
                pass


class ParseKafkaMessage(beam.DoFn):
    def process(self, element):
        try:
            val = element[1]
            if isinstance(val, bytes):
                val = val.decode('utf-8')
            event = json.loads(val)
            required = {'event_id', 'key', 'event_time', 'payload'}
            if not required.issubset(event.keys()):
                yield beam.pvalue.TaggedOutput('invalid', (element, "missing_fields"))
                return
            yield event
        except Exception as e:
            yield beam.pvalue.TaggedOutput('invalid', (element, str(e)))


def assign_event_time(event):
    dt = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
    return TimestampedValue((event['key'], event), dt.timestamp())


class DeduplicateRFID(beam.DoFn):
    SEEN_IDS = SetStateSpec('seen_ids', StrUtf8Coder())
    EXPIRY = TimerSpec('expiry', TimeDomain.WATERMARK)

    def process(self, element, seen_ids=beam.DoFn.StateParam(SEEN_IDS),
                expiry=beam.DoFn.TimerParam(EXPIRY), window=beam.DoFn.WindowParam):
        checkpoint, event = element
        event_id = event['event_id']

        seen_set = set(seen_ids.read())
        if event_id not in seen_set:
            seen_ids.add(event_id)
            expiry.set(window.end)
            yield element

    @on_timer(EXPIRY)
    def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
        seen_ids.clear()


class IncrementalCount(beam.DoFn):

    COUNT_STATE = ReadModifyWriteStateSpec('count', VarIntCoder())

    def process(self, element, count_state=beam.DoFn.StateParam(COUNT_STATE), window=beam.DoFn.WindowParam):
        checkpoint, _ = element
        current = count_state.read() or 0
        current += 1
        count_state.write(current)

        window_start = window.start.to_utc_datetime().isoformat().replace('+00:00', 'Z')
        yield (checkpoint, current, window_start)


class WriteToKafkaTopic(beam.DoFn):
    def setup(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            acks='all',
            retries=3
        )

    def process(self, element):
        checkpoint, count, window_start = element

        output = {
            "idempotency_key": f"{checkpoint}|{window_start}",
            "checkpoint": checkpoint,
            "window_start": window_start,
            "total_runners": count
        }

        try:
            future = self.producer.send('traffic.alerts', key=output["checkpoint"], value=output)
            record_metadata = future.get(timeout=10)
            print(f"\n[ALERTA TRÁFICO - CONFIRMADO EN KAFKA] partition={record_metadata.partition} offset={record_metadata.offset} -> {output}")
        except Exception as e:
            print(f"[ERROR AL ENVIAR A KAFKA] {e}")

        yield output

    def teardown(self):
        self.producer.flush()
        self.producer.close()


def run_pipeline():
    options = PipelineOptions(['--streaming'])
    print("Iniciando Pipeline End-to-End ()...")
    print("Presioná Ctrl+C para detener y ver el resumen final por checkpoint.\n")

    with beam.Pipeline(options=options) as p:

        parsed = (
            p
            | "Impulso Periódico" >> PeriodicImpulse(fire_interval=2, apply_windowing=False)
            | "Leer de Kafka" >> beam.ParDo(ReadFromKafkaPython())
            | "Parsear JSON" >> beam.ParDo(ParseKafkaMessage()).with_outputs('invalid', main='valid')
        )

        parsed.invalid | "Loggear Inválidos" >> beam.Map(
            lambda x: logging.warning(f"[EVENTO INVÁLIDO] razón={x[1]} elemento={x[0]}")
        )

        deduplicados = (
            parsed.valid
            | "Asignar Event Time" >> beam.Map(assign_event_time)
            | "Ventanas Fijas (1 min)" >> beam.WindowInto(
                FixedWindows(60),
                allowed_lateness=Duration(seconds=120)
            )
            | "Deduplicar" >> beam.ParDo(DeduplicateRFID()).with_input_types(Tuple[str, Any])
        )

        # Rama 1: conteo global acumulado (para el resumen final)
        deduplicados | "Registrar Conteo Global" >> beam.Map(registrar_evento_global)

        # Rama 2: conteo por ventana (para las alertas periódicas a Kafka)
        (
            deduplicados
            | "Quedarnos con clave" >> beam.Map(lambda kv: (kv[0], 1))
            | "Contar Incrementalmente" >> beam.ParDo(IncrementalCount()).with_input_types(Tuple[str, int])
            | "Salida a Kafka y Consola" >> beam.ParDo(WriteToKafkaTopic())
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger('apache_beam.transforms.core').setLevel(logging.ERROR)
    signal.signal(signal.SIGINT, _manejar_interrupcion)

    try:
        run_pipeline()
    except (KeyboardInterrupt, RuntimeError):
        print("\nDeteniendo pipeline...")
    finally:
        imprimir_resumen_final()
        print("Pipeline detenido correctamente.")