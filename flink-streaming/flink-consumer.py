from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

env = StreamExecutionEnvironment.get_execution_environment()
# the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues

deserialization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(type_info= Types.ROW_NAMED(field_names= ['id', 
                                              'value', 
                                              'type', 
                                              'timestamp'], 
                                field_types= [
                                    Types.INT(), \
                                    Types.INT(),\
                                    Types.STRING(),\
                                    Types.LONG()\
        ])
    ).build()

kafka_consumer = FlinkKafkaConsumer(
    topics='hieuung',
    deserialization_schema=deserialization_schema,
    properties={
        'bootstrap.servers': 'kafka:9092', 
        'group.id': 'test_group'
        }
    )

ds = env.add_source(kafka_consumer)

ds.print()

env.execute()