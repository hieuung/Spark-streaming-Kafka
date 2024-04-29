import logging
import sys
import time
from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema

input_type_info = Types.ROW_NAMED(field_names= ['id', 
                                              'value', 
                                              'type', 
                                              'timestamp'], 
                                field_types= [
                                    Types.INT(), \
                                    Types.INT(),\
                                    Types.STRING(),\
                                    Types.LONG()\
        ])

output_type_info = Types.ROW_NAMED(field_names= ['id', 
                                              'value', 
                                              'type', 
                                              'funcx', 
                                              'timestamp'], 
                                field_types= [
                                    Types.INT(), \
                                    Types.INT(),\
                                    Types.STRING(),\
                                    Types.INT(),\
                                    Types.LONG()\
        ])

def current_milli_time():
    return round(time.time() * 1000)

def write_to_kafka(env, ds):
    type_info = output_type_info

    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()
    
    kafka_producer = FlinkKafkaProducer(
        topic='flink-sink',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'kafka:9092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)

def read_from_kafka(env):
    type_info = input_type_info
    
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(type_info) \
        .build()
    
    kafka_consumer = FlinkKafkaConsumer(
        topics='hieuung',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'test_group'}
    )

    ds = env.add_source(kafka_consumer)
    return ds

def transform_row(row):
    id = row['id']
    value = row['value']
    type = row['type']
    timestamp = current_milli_time()
    fx = (3*row.value + 1) if row.type == 'odd' else row.value % 2
    return Row(id= id, \
            value= value, \
            type= type, \
            funcx= fx, \
            timestamp= timestamp)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()

    print("start reading data from kafka")
    ds = read_from_kafka(env)

    ds = ds.map(transform_row, output_type= output_type_info)
    ds.print('stdout')

    print("start writing data to kafka")
    write_to_kafka(env, ds)
    env.execute('Datastream-test')
