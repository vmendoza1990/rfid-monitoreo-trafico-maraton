import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.trigger import Repeatedly, AfterProcessingTime, AccumulationMode, AfterCount
from apache_beam.utils.timestamp import Duration

from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer


class ReadFromKafkaPython(beam.DoFn):
    """
    Lector de Kafka en Python con manejo seguro de file descriptors en Windows.
    """
    def _create_consumer(self):
        from kafka import KafkaConsumer
        return KafkaConsumer(
            'rfid.reads',
            bootstrap_servers=['127.0.0.1:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='marathon-pipeline-group-v4',
            value_deserializer=lambda v: v,
            key_deserializer=lambda k: k,
        )

    def setup(self):
        self.consumer = self._create_consumer()
        logging.warning("[KAFKA-READER] Consumidor iniciado, esperando mensajes...")

    def process(self, _):
        records = {}
        try:
            records = self.consumer.poll(timeout_ms=800, max_records=500)
        except Exception as e:
    
            logging.warning(f"[KAFKA-READER] Fallo de socket detectado ({e}). Reiniciando conexión para el próximo impulso...")
            try:
                self.consumer.close(autocommit=False)
            except:
                pass
            self.consumer = self._create_consumer()
            return 

        count = 0
        for _tp, messages in records.items():
            for message in messages:
                count += 1
                yield (message.key, message.value)
        
        if count:
            logging.info(f"[KAFKA-READER] Poll completado: {count} mensajes")

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


class WriteToKafkaTopic(beam.DoFn):
    def setup(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            acks='all',
            retries=3
        )

    def process(self, element, window=beam.DoFn.WindowParam):
        checkpoint, count = element
        window_start = window.start.to_utc_datetime().isoformat().replace('+00:00', 'Z')

        output = {
            "idempotency_key": f"{checkpoint}|{window_start}",
            "checkpoint": checkpoint,
            "window_start": window_start,
            "total_runners": count
        }

        self.producer.send('traffic.alerts', key=output["checkpoint"], value=output)
        print(f"\n[ALERTA TRÁFICO - VENTANA CERRADA] -> {output}")
        yield output

    def teardown(self):
        self.producer.flush()
        self.producer.close()


def debug_and_pass(event):
    print(f"[DEBUG LECTURA KAFKA] -> {event['key']} | Runner: {event['payload']['runner_id']}")
    return event

def run_pipeline():
    options = PipelineOptions(['--streaming'])

    print("Iniciando Pipeline End-to-End conectado a Kafka ...")

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

        (
            parsed.valid
            | "Verificar Lectura" >> beam.Map(debug_and_pass)
            | "Asignar Event Time" >> beam.Map(assign_event_time)
            | "Ventanas Fijas (1 min)" >> beam.WindowInto(
                FixedWindows(15),
                #allowed_lateness=Duration(seconds=120),
                trigger=Repeatedly(AfterProcessingTime(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=Duration(seconds=120)
            )
            | "Deduplicar" >> beam.ParDo(DeduplicateRFID())
            | "Quedarnos con clave" >> beam.Map(lambda kv: (kv[0], 1))
            | "Sumar por Checkpoint" >> beam.CombinePerKey(sum)
            | "Salida a Kafka y Consola" >> beam.ParDo(WriteToKafkaTopic())
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run_pipeline()