import json
import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka

class Desirializer:
    @staticmethod
    def convert_kafka_record_to_dictionary(record):
        logging.info(record)
        # the records have 'value' attribute when --with_metadata is given
        if hasattr(record, 'value'):
            ride_bytes = record.value
        elif isinstance(record, tuple):
            ride_bytes = record[1]
        else:
            raise RuntimeError('unknown record type: %s' % type(record))
        # Converting bytes record from Kafka to a dictionary.
        mess_info = json.loads((ride_bytes.decode("UTF-8")))
        logging.info(mess_info)
        return mess_info

def run():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--bootstrap_servers',
        dest='bootstrap_servers',
        required=True,
        help='Bootstrap servers for the Kafka cluster. Should be accessible by '
        'the runner')
        
    parser.add_argument(
        '--topics', 
        dest='topics', 
        required=True,
        type=lambda s: [item for item in s.split(',')],
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

    p = beam.Pipeline(options=options)

    (p
    | 'Read from Kafka' >> ReadFromKafka(
                                consumer_config= consumer_config,
                                topics= known_args.topics,
                                with_metadata= known_args.with_metadata)

    | "Byte to dict" >> beam.Map(lambda record: Desirializer.convert_kafka_record_to_dictionary(record))
    )

    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()