import apache_beam as beam
import os

class ParseAndFilterLogs(beam.DoFn):
    """Custom ParDo operation for finding system anomalies"""
    def process(self, element):
        parts = [item.strip() for item in element.split('|')]
        if len(parts) == 3:
            timestamp, level, message = parts[0], parts[1].upper(), parts[2]
            # Triage filter: Only forward severe errors to warehouse
            if level in ['CRITICAL', 'ERROR']:
                yield f"{timestamp}, {level}, {message}"

def run():
    output_dir = r"C:\Users\HP\Documents\GitHub\gcp-data-pipeline-simulation\transformation_layer"
    output_prefix = os.path.join(output_dir, "cleaned_incidents")

    with beam.Pipeline(runner="DirectRunner") as pipeline:
        (
            pipeline
            | 'Mocking Raw Log Extraction' >> beam.Create([
                "2026-06-07 10:00 | INFO | System health stable.",
                "2026-06-07 10:02 | ERROR | Connection to buffer pool timed out.",
                "2026-06-07 10:05 | DEBUG | Packet payload sent successfully.",
                "2026-06-07 10:12 | CRITICAL | Deadlock detected on main cluster."
            ])
            | 'Execute ParDo Filtration' >> beam.ParDo(ParseAndFilterLogs())
            | 'Load To Clean Layer' >> beam.io.WriteToText(output_prefix, file_name_suffix = '.csv', shard_name_template='')
        )

if __name__ == '__main__':
    run()