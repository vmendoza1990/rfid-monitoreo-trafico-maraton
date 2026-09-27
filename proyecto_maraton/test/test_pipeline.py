import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.transforms.userstate import SetStateSpec, TimerSpec, on_timer
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.coders import StrUtf8Coder
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AccumulationMode
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime
import logging


def assign_event_time(event):
    dt = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
    return TimestampedValue((event['key'], event), dt.timestamp())

class DeduplicateRFID(beam.DoFn):
    SEEN_IDS = SetStateSpec('seen_ids', StrUtf8Coder())
    EXPIRY = TimerSpec('expiry', TimeDomain.WATERMARK)

    def process(self, element, seen_ids=beam.DoFn.StateParam(SEEN_IDS), expiry=beam.DoFn.TimerParam(EXPIRY), window=beam.DoFn.WindowParam):
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

class FormatOutput(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        checkpoint, count = element
        window_start = window.start.to_utc_datetime().isoformat()
        yield {
            "idempotency_key": f"{checkpoint}|{window_start}",
            "checkpoint": checkpoint,
            "window_start": window_start,
            "total_runners": count
        }

def run_test():
    print("Iniciando prueba local del pipeline (TestPipeline)...")
    
    mock_events = [
        {"event_id": "UUID-1", "key": "KM-10", "event_time": "2026-09-26T12:00:10Z", "payload": {"runner": "R-01"}},
        {"event_id": "UUID-1", "key": "KM-10", "event_time": "2026-09-26T12:00:12Z", "payload": {"runner": "R-01"}},
        {"event_id": "UUID-2", "key": "KM-10", "event_time": "2026-09-26T12:00:45Z", "payload": {"runner": "R-02"}},
    ]

    with TestPipeline() as p:
        (
            p 
            | "Crear Eventos" >> beam.Create(mock_events)
            | "Asignar Event Time" >> beam.Map(assign_event_time)
            | "Ventana Fija de 1 Minuto" >> beam.WindowInto(FixedWindows(60))
            | "Deduplicar por event_id" >> beam.ParDo(DeduplicateRFID())
            | "Extraer clave para conteo" >> beam.Map(lambda kv: (kv[0], 1))
            | "Contar Corredores por Checkpoint" >> beam.CombinePerKey(sum)
            | "Formatear a Salida Idempotente" >> beam.ParDo(FormatOutput())
            | "Imprimir Resultados" >> beam.Map(print)
        )

def run_test_stream_lateness():
    """
    Prueba el comportamiento del pipeline ante eventos tardíos (late data)
    usando TestStream, respetando el horizonte de allowed_lateness configurado.
    """
    print("\nIniciando prueba de ventanas y lateness (TestStream)...")

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    evento_a_tiempo = ("KM-10", {
        "event_id": "UUID-A",
        "key": "KM-10",
        "event_time": "2026-09-26T12:00:10Z",
        "payload": {"runner": "R-A"}
    })

    evento_tardio = ("KM-10", {
        "event_id": "UUID-B",
        "key": "KM-10",
        "event_time": "2026-09-26T12:00:20Z",
        "payload": {"runner": "R-B"}
    })

    test_stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([evento_a_tiempo], event_timestamp=10)
        .advance_watermark_to(100)   # <-- CORREGIDO: tarde (>60) pero dentro de allowed_lateness=120 (límite=180)
        .add_elements([evento_tardio], event_timestamp=20)
        .advance_watermark_to_infinity()
    )

    with TestPipeline(options=options) as p:
        resultado = (
            p
            | "Inyectar Stream" >> test_stream
            | "Ventana Fija con Lateness" >> beam.WindowInto(
                FixedWindows(60),
                allowed_lateness=120,
                trigger=AfterWatermark(late=AfterCount(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING
            )
            | "Deduplicar" >> beam.ParDo(DeduplicateRFID())
            | "Extraer clave" >> beam.Map(lambda kv: (kv[0], 1))
            | "Contar por Checkpoint" >> beam.CombinePerKey(sum)
        )

        assert_that(resultado, equal_to([("KM-10", 1), ("KM-10", 2)]))

    print("Prueba de lateness completada: el evento tardío se computó dentro del horizonte permitido.")

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run_test()
    run_test_stream_lateness()