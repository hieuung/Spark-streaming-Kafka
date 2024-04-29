# To use the kafka
from kafka import KafkaProducer
import logging
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def current_milli_time():
    return round(time.time() * 1000)

if __name__ == '__main__':
    while 1 == 1:
        try:
            producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
            logger.info("connnected!")
            break
        except Exception as e:
            logger.error(e)
            logger.info('Retry')
            time.sleep(5)

    i = 1
    while 1 == 1:
        logger.info("resend!")
        now = current_milli_time()
        type = 'even' if i % 2 == 0 else 'odd'
        payload = {
            "id" : i,
            "value" : i,
            "type" : type,
            "timestamp" : now,
        }
        payload_dump = json.dumps(payload, ensure_ascii=False)
        try:
            producer.send('hieuung', 
                        key= bytes(type, encoding= 'utf-8'),
                        value= bytes(payload_dump, encoding= 'utf-8'), 
                        partition= i % 2)
            logger.info("published!")
        except Exception as e:
            logger.error(e)
        i += 1
        time.sleep(5)