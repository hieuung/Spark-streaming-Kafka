# To use the kafka
from kafka import KafkaConsumer
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    while 1 == 1:
        try:
            consumer = KafkaConsumer(
                'hieuung',
                bootstrap_servers=['kafka:9092'],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='hieu-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8')
                )
            for message in consumer:
                logger.info('Message:' + str(message))
            break
        except Exception as e:
            logger.error(e)
            logger.info('Retry')
