import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka

def run():
    parser = argparse.ArgumentParser()

    parser.add_argument(
      '--bootstrap_servers',
      dest='bootstrap_servers',
      required=True,
      help='Bootstrap servers for the Kafka cluster. Should be accessible by '
      'the runner')
    parser.add_argument(
        '--topic',
        dest='topic',
        default='hieuung',
        help='Kafka topic to write to and read from')
    parser.add_argument(
        '--with_metadata',
        default=False,
        action='store_true',
        help='If set, also reads metadata from the Kafka broker.')

    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args)

    consumer_config = {
         'bootstrap.servers': known_args.bootstrap_servers
    }

    p = beam.Pipeline(options)

    (p
    | 'ReadFromKafka' >> ReadFromKafka(consumer_config= consumer_config,
                              topic= known_args.tocpic,
                              with_metadata= known_args.with_metadata)
    )

if __name__ == '__main__':
    run()