"""
    DESIGNED DOCUMENTATION: https://fundiin.atlassian.net/wiki/x/jQCQDg
"""

import os
import logging
import argparse
import json
import base64
from typing import Any
import psycopg2
import re

import apache_beam as beam
from apache_beam import DoFn, Partition, Pipeline, ParDo
from time import perf_counter
from functools import wraps
from datetime import datetime
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Decorators =======================================================================
def getter_try_except(func):
    """Decorator to call a function and log any exceptions.

    This decorator wraps a function and catches any exceptions that occur when calling the function. If an exception occurs, it is logged at the ERROR level, along with the traceback and stack info. It then returns a default value of 0.

    Parameters:
        func (function): The function to wrap and catch exceptions for.

    Returns:
        function: The wrapped function. 
    """
    
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logging.error(f'{e} occured in {func.__name__}')
            logging.debug(f'{e} occured in {func.__name__}', exc_info=True, stack_info=True)
            # # Check if PDB is being used
            # if 'pdb' in sys.argv:
            #     breakpoint()
            return 0
    return wrapped

def timeit(func):
    """Decorator to time a function and print the elapsed time.

    This decorator uses the perf_counter to time a function and prints the total elapsed time in seconds after the function completes. It wraps the function to time and returns the result of the function.

    Parameters:
        func (function): The function to wrap and time.

    Returns:
        function: The wrapped, timed function. 
    """
    
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = perf_counter()
        result = func(*args, **kwargs)
        end_time = perf_counter()
        total_time = end_time - start_time
        print(f'Query {args} took {total_time:.4f} seconds')
        return result
    return timeit_wrapper
# Decorators =======================================================================

class SchemaUtilsFunction:
    def __init__(self, filename="") -> None:
        if filename != "":
            dir_path = os.path.dirname(os.path.realpath(__file__))
            schema_file = os.path.join(dir_path, 'resources', filename)
            with open(schema_file) \
                    as f:
                data = f.read()
                # Wrapping the schema in fields is required for the BigQuery API.
                self.schema_data = data
        else:
            self.schema_data = """
            [        
                {
                    "name": "rank",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },           
                {
                    "name": "ref_order_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "pre_booking_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "order_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {   
                    "name": "reason_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {   
                    "name": "created_time",
                    "type": "TIMESTAMP",
                    "mode": "NULLABLE"
                },
                {   
                    "name": "status",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {   
                    "name": "previous_status",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {   
                    "name": "action_by",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "register_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "customer_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "phone_number",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "item",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields" : [
                        {
                        "name": "name",
                        "type": "STRING",
                        "mode": "NULLABLE"
                        },
                        {
                        "name": "quantity",
                        "type": "INTEGER",
                        "mode": "NULLABLE"
                        }
                    ]
                },
                {
                    "name": "emc_item",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "merchant_id",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "merchant",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "order_value",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },
                {
                    "name": "zs_source",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "zs_terminal",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "zs_method",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "voucher",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "lender",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "payment_type",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "order_type",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "promotion_value",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },
                {
                    "name": "down_payment",
                    "type": "FLOAT",
                    "mode": "NULLABLE"
                },
                {
                    "name": "payment_method",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "user_type",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },

            {"name": "recon_merchant_id", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_mdr_gross", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_begin_time", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_cycle", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_refund_timeline", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_referral_commission", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_refund_fee", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_debt_exemption", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_created_time", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_send_email_type", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_export_tcp_type", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_vat", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_mdr_gross_plus_six_series", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_mdr_gross_plus_nine_series", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_mdr_gross_plus_twelve_series", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_initiate_reconciliation_method", "type": "STRING", "mode": "NULLABLE"},
            {"name": "recon_is_deduction_first_series_in_calculate_commission", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_is_received_contact", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_weekend_refund_time", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_holiday_refund_time", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_apply_refund_time", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "recon_updated_date", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "merchant_bnpl_plus_merchant_service_fee_percent_six_phase", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "merchant_bnpl_plus_merchant_service_fee_percent_nine_phase", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "merchant_bnpl_plus_merchant_service_fee_percent_twelve_phase", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "merchant_bnpl_plus_service_fee_percent_six_phase", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "merchant_bnpl_plus_service_fee_percent_nine_phase", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "merchant_bnpl_plus_service_fee_percent_twelve_phase", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "reconciliation_config", "type": "RECORD", "mode": "REPEATED", "fields": 
                [
                    {"name": "field", "type": "STRING", "mode": "NULLABLE"}, 
                    {"name": "operation", "type": "STRING", "mode": "NULLABLE"}, 
                    {"name": "operation_value", "type": "NUMERIC", "mode": "NULLABLE"}, 
                    {"name": "value", "type": "NUMERIC", "mode": "NULLABLE"}, 
                    {"name": "effective_date", "type": "NUMERIC", "mode": "NULLABLE"}, 
                    {"name": "expiration_date", "type": "NUMERIC", "mode": "NULLABLE"}, 
                    {"name": "created_date", "type": "NUMERIC", "mode": "NULLABLE"}, 
                    {"name": "updated_date", "type": "NUMERIC", "mode": "NULLABLE"}, 
                    {"name": "is_deleted", "type": "NUMERIC", "mode": "NULLABLE"}
                ]
                }
            ]
        """

    def get_schema(self):
        return self.schema_data

    def get_col_name(self):
        schema_data = self.parse_table_schema_from_json()
        result = []
        for i in schema_data._Message__tags[1]:
            result.append(i.name)
        return result

    def parse_table_schema_from_json(self):
        from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
        schema = parse_table_schema_from_json('{"fields": ' + self.schema_data + '}')
        return schema
    
class PostgresQueryFn:
    def __init__(self, fetch_size= 1000):
        self.fetch_size=fetch_size
        self.database = os.getenv("DATABASE_NAME")
        self.host = os.getenv("HOST")
        self.username = os.getenv("POSTGRE_DEV_USERNAME")
        self.password = os.getenv("POSTGRE_DEV_PASSWORD")

    @timeit
    def query(self, query):
        with psycopg2.connect(database= self.database, 
                              user=self.username, 
                              password=self.password, 
                              host=self.host, 
                              port= '5432') as conn:
            result = []
            with conn.cursor() as cur:
                cur.itersize = self.fetch_size
                cur.execute(query)
                col_name = cur.description
                for record in cur:
                    result.append({k.name: v for v, k in zip(record, col_name)})
            return result

class GetReconciliationConfigWithMerchantId:
    def __init__(self, dev_env):
        self.reconciliation_cols = [
            'merchant_id',
            'mdr_gross',
            'begin_time',
            'cycle',
            'refund_timeline',
            'referral_commission',
            'refund_fee',
            'debt_exemption',
            'created_time',
            'send_email_type',
            'export_tcp_type',
            'vat',
            'mdr_gross_plus_six_series',
            'mdr_gross_plus_nine_series',
            'mdr_gross_plus_twelve_series',
            'initiate_reconciliation_method',
            'is_deduction_first_series_in_calculate_commission',
            'is_received_contact',
            'weekend_refund_time',
            'holiday_refund_time',
            'apply_refund_time',
            'updated_date'
            ]
        
        self.reconciliation_config_cols = [
            'field',
            'operation',
            'operation_value',
            'value',
            'effective_date',
            'expiration_date',
            'created_date',
            'updated_date',
            'is_deleted'
            ]

        self.merchant_cols = [
            'bnpl_plus_merchant_service_fee_percent_six_phase',
            'bnpl_plus_merchant_service_fee_percent_nine_phase',
            'bnpl_plus_merchant_service_fee_percent_twelve_phase',
            'bnpl_plus_service_fee_percent_six_phase',
            'bnpl_plus_service_fee_percent_nine_phase',
            'bnpl_plus_service_fee_percent_twelve_phase'
            ]

        reconciliation_add_on_select = ['reconciliation.' + col + ' recon_' + col for col in self.reconciliation_cols]
        reconciliation_config_add_on_select = ['reconciliation_config.' + col + ' recon_config_' + col for col in self.reconciliation_config_cols]
        merchant_add_on_select = ['merchant.' + col + ' merchant_' + col for col in self.merchant_cols]

        concat_select_add_on = ',\n'.join(reconciliation_add_on_select + reconciliation_config_add_on_select + merchant_add_on_select)
        self.db_query = f"""
                SELECT 
                    {concat_select_add_on}
                FROM
                    fundiin_api_{dev_env}.reconciliation 
                    JOIN fundiin_api_{dev_env}.merchant ON merchant.id = reconciliation.merchant_id
                    LEFT JOIN fundiin_api_{dev_env}.reconciliation_config ON reconciliation_config.reconciliation_id = reconciliation.id
            """ 
        
        reconciliation_add_on_select = ['pg_typeof(reconciliation.' + col + ') recon_' + col for col in self.reconciliation_cols]
        reconciliation_config_add_on_select = ['pg_typeof(reconciliation_config.' + col + ') recon_config_' + col for col in self.reconciliation_config_cols]
        merchant_add_on_select = ['pg_typeof(merchant.' + col + ') merchant_' + col for col in self.merchant_cols]

        self.db_query_schema = f"""
            WITH raw AS (
                SELECT 
                    {','.join(reconciliation_add_on_select + reconciliation_config_add_on_select + merchant_add_on_select)}
                FROM
                    fundiin_api_{dev_env}.reconciliation 
                    JOIN fundiin_api_{dev_env}.merchant ON merchant.id = reconciliation.merchant_id
                    LEFT JOIN fundiin_api_{dev_env}.reconciliation_config ON reconciliation_config.reconciliation_id = reconciliation.id
                )
                SELECT
                    DISTINCT *
                FROM
                    raw
            """ 

        self.query_class = PostgresQueryFn()

    def query(self, id):
        condition = f"""WHERE\nmerchant.id = {id}"""
        result = self.query_class.query(self.db_query + condition)
        return result
    
    def get_schema(self):
        result = self.query_class.query(self.db_query_schema)
        return result

class MappingSource:
    def __init__(self) -> None:
        _list_SHOPIFY = ['SHOPIFY']
        _list_WOOCOMMERCE = ['WOOCOMMERCE']
        _list_SAPO = ['SAPOVN', 'SAPO']
        _list_HARAVAN = ['HARAVAN']
        _list_PANCAKE = ['PANCAKE']
        _list_MAGENTO = ['MAGENTOWEB', 'MAGENTO']
        _list_NHANHVN = ['NHANHVN']
        _emc = ['EMC']
        _qr = ['STATIC_QR', 'QR']
        _token = ['GALAXYPLAY']

        self.dict_map = {
            'webhook' : _list_SHOPIFY + _list_WOOCOMMERCE + _list_SAPO + _list_HARAVAN + _list_PANCAKE + _list_MAGENTO + _list_NHANHVN,
            'emc' : _emc,
            'token': _token,
            'qr' : _qr
        }

    def get_zs_source(self, input):
        if input is None:
            return 'emc'
        for k, v in self.dict_map.items():
            if input in v:
                return k + (f'({input})' if k == 'webhook' else '')
        return 'api_integration' + f'({input})'
    
class MappingOrderType:
    def __init__(self) -> None:
        self._bnpl = (1, 2, 3, 7, 8, 9)
        self._bnpl_plus = (4, 5, 6)

    def _get_order_type(self, x):
        if x in self._bnpl:
            return 'BNPL'
        if x in self._bnpl_plus:
            return 'BNPL+'
        return None
    
    def get_order_type(self, downpayment, total_value, paynow_check, type):
        if downpayment is None:
            return self._get_order_type(type)
        return 'PAYNOW' if (downpayment == total_value and paynow_check == 1) else self._get_order_type(type)
    
class PaymentGateWay:
    def __init__(self) -> None:
        pass

class MappingPaymentType:
    def __init__(self) -> None:
        self._mapping = {
            None : 'Not pick yet',
            0 : 'Not pick yet',
            1 : 'Pay in 3 monthly installments',
            2 : 'Pay in 30 days',
            3 : 'Pay in 6 monthly installments',
            4 : 'Pay in 6 monthly',
            5 : 'Pay in 9 months',
            6 : 'Pay in 12 months',
            7 : 'Pay in 2 weeks',
            8 : 'Pay in 1 week',
            9 : 'Pay in 2 months'
        }

    def get_payment(self, x):
        return self._mapping[x]

class QueryInfoFromDatabase:
    def __init__(self, dev_env):
        self._merchant_name_query = f"""
            WITH merchant AS (
                SELECT
                    id,
                    name merchant
                FROM
                    fundiin_api_{dev_env}.merchant
            )
            SELECT
                merchant.merchant
            FROM
                merchant
        """

        self._merchant_name_with_shopid_query = f"""
            WITH merchant_online_shop AS (
                SELECT
                    shop_id,
                    merchant_id
                FROM
                    fundiin_api_{dev_env}.merchant_online_shop
            ),
             WITH merchant AS (
                SELECT
                    id,
                    name merchant
                FROM
                    fundiin_api_{dev_env}.merchant
            )
            SELECT
                merchant_online_shop.merchant_id,
                merchant.merchant
            FROM
                merchant_online_shop
                JOIN merchant ON merchant.id = merchant_online_shop.merchant_id
        """

        self._register_from_prebooking_id_query = f"""
            WITH pre_booking AS (
                SELECT
                    id,
                    phone_number,
                    merchantid
                FROM
                    fundiin_api_{dev_env}.pre_booking
            ),
            register AS (
                SELECT
                    id,
                    phone_number
                FROM
                    fundiin_api_{dev_env}.register
            )
                SELECT
                    pre_booking.id,
                    pre_booking.merchant_id,
                    register.id register_id
                FROM
                    pre_booking 
                    register ON register.phone_number = pre_booking.phone_number
            """ 
        
        self._booking_auto_reject_reason_from_booking_id_query = f"""
            WITH course_booking AS (
                    SELECT
                        id,
                        phone_number,
                        merchantid
                    FROM
                        fundiin_api_{dev_env}.pre_booking
                ),
                booking_review_history AS (
                    SELECT
                        *,
                        row_number() over (PARTITION BY booking_id ORDER BY created_date ASC) rn
                    FROM
                        fundiin_api_{dev_env}.booking_review_history brh
                )
                SELECT
                    course_booking.id,
                    booking_review_history.*
                FROM
                    course_booking 
                    booking_review_history ON (course_booking.id = booking_review_history.booking_id AND booking_review_history.rn = 1)
        """

        self._booking_manual_reject_reason_from_booking_id_query = f"""
            WITH course_booking AS (
                    SELECT
                        id,
                        phone_number,
                        merchantid
                    FROM
                        fundiin_api_{dev_env}.pre_booking
                ),
                booking_note AS (
                    SELECT
                        booking_note.booking_id,
                        booking_note.content content,
                        note_category.content manual_content
                    FROM
                        fundiin_api_{dev_env}.booking_note 
                        LEFT JOIN fundiin_api_{dev_env}.note_category ON note_category.id = booking_note.category_id
                    WHERE
                        booking_note.type = 'REJECT'
                )
                SELECT
                    course_booking.id,
                    booking_note.content,
                    booking_note.manual_content
                FROM
                    course_booking 
                    LEFT JOIN booking_note ON (course_booking.id = booking_note.booking_id)
        """

        self._payment_from_booking_id_query = f"""
            WITH course_booking AS (
                    SELECT
                        id,
                        phone_number,
                        merchantid
                    FROM
                        fundiin_api_{dev_env}.course_booking
                ),
                booking_series AS (
                    SELECT
                        id,
                        bookingid,
                        payment_method
                    FROM
                        fundiin_api_{dev_env}.booking_series
                )
                SELECT
                    course_booking.id,
                    booking_series.payment_method
                FROM
                    course_booking 
                    booking_series ON course_booking.id = booking_series.bookingid
        """

        self._prebooking_id_from_booking_id_query = f"""
            WITH pre_booking AS (
                    SELECT
                        id,
                        booking_id
                    FROM
                        fundiin_api_{dev_env}.pre_booking
                )
                SELECT
                    id
                FROM
                    pre_booking
        """

        self._mapping_to_temp_query = f"""
             WITH temp AS (
                SELECT
                    data::json->'order'->>'order_id' temp_order_id
                FROM
                    fundiin_api_{dev_env}.temp 
                WHERE
                    data like '%order_id%'
                    AND data not like '%GALAXYPLAY%'
            )
            SELECT
                *
            FROM
                temp
    ),
        """
        
        self.query_class = PostgresQueryFn()

    def query_register(self, id):
        condition = f"""WHERE\npre_booking.id = '{id}'"""
        result = self.query_class.query(self._register_from_prebooking_id_query + condition)
        return result
    
    def query_payment(self, booking_id):
        condition = f"""WHERE\ncourse_booking.id = '{booking_id}'"""
        result = self.query_class.query(self._payment_from_booking_id_query + condition)
        return result
    
    def query_reject_reason(self, booking_id):
        condition = f"""WHERE\ncourse_booking.id = {booking_id}"""
        result = self.query_class.query(self._booking_reject_reason_from_booking_id_query + condition)
        return result
    
    def query_manual_reject_reason(self, booking_id):
        condition = f"""WHERE\ncourse_booking.id = '{booking_id}'"""
        result = self.query_class.query(self._booking_manual_reject_reason_from_booking_id_query + condition)
        return result
    
    def query_mapping_to_temp(self, temp_order_id):
        condition = f"""WHERE\ntemp.temp_order_id = '{temp_order_id}'"""
        result = self.query_class.query(self._mapping_to_temp_query + condition)
        return result

    def query_merchant_name(self, merchant_id):
        condition = f"""WHERE\nmerchant.id = {merchant_id}"""
        result = self.query_class.query(self._mapping_to_temp_query + condition)
        return result

    def query_pre_booking_id(self, booking_id):
        condition = f"""WHERE\npre_booking.booking_id = '{booking_id}'"""
        result = self.query_class.query(self._prebooking_id_from_booking_id_query + condition)
        return result

    def query_merchant_name_with_shopid(self, shop_id):
        condition = f"""WHERE\merchant_online_shop.shop_id = {shop_id}"""
        result = self.query_class.query(self._merchant_name_with_shopid_query + condition)
        return result

class CDCMessageParse:
    def __init__(self) -> None:
        self.choose_operator_mapping = {
            'u': 'update',
            'c' : 'created'
        }

        self.query_class = QueryInfoFromDatabase(dev_env= os.getenv('DEV_ENV'))

        self.change_to_datetime_fn = lambda x: datetime.fromtimestamp(x / 1000)

    def filter_op(self, mess_info):
        condition1 = mess_info['payload']['op'] in list(self.choose_operator_mapping.keys())

        if not condition1:
            logging.info('Discard ' + mess_info['payload']['op'] + ' operator')
            return False
        return True

    def _convert_to_int(self, v):
        if isinstance(v, str):
            value = v
            scale = 1
        else:
            value = v['value']
            scale = v['scale']
        decoded_bytes = base64.b64decode(value)
        integer_value = int.from_bytes(decoded_bytes, byteorder='big')
        return (integer_value / scale) if scale > 0 else integer_value

    def convert_kafka_record_to_dictionary(self, record):
        # the records have 'value' attribute when --with_metadata is given
        if hasattr(record, 'value'):
            ride_bytes = record.value
        elif isinstance(record, tuple):
            ride_bytes = record[1]
        else:
            raise RuntimeError('unknown record type: %s' % type(record))
        # Converting bytes record from Kafka to a dictionary.
        mess_info = json.loads((ride_bytes.decode("UTF-8")))

        mess_schema = mess_info['schema']['fields']
        mess_value = mess_info['payload']

        before_type = [ele['type'] for ele in mess_schema[0]['fields']]
        after_type = [ele['type'] for ele in mess_schema[1]['fields']]

        operation = mess_value['op']

        if operation == 'u' and mess_value['before'] is not None:
            before_value = {k:(self._convert_to_int(v) if (t in ('struct', 'bytes') and v is not None) else v) for ((k, v), t) in zip(mess_value['before'].items(), before_type)}
        else:
            before_value = None
        
        after_value = {k: (self._convert_to_int(v) if (t in ('struct', 'bytes') and v is not None) else v) for ((k, v), t) in zip(mess_value['after'].items(), after_type)}

        mess_info['payload']['before'] = before_value
        mess_info['payload']['after'] = after_value

        return mess_info
    
    def get_change_column_values(self, mess_info):
        payload = mess_info['payload']
        before = payload['before']
        after = payload['after']
        list_col_changed = {}
        for key_common in after.keys():
            try:
                before_value = before[key_common]
            except:
                before_value = None
            after_value = after[key_common]
            if after_value != before_value:
                list_col_changed[key_common] = {'before': before_value, 'after': after_value}
        return {
                'list_col_changed': list_col_changed,
                'after': after, 
                'source':payload['source']['table'], 
                'operator':payload['op'],
                'event_time': payload['source']['ts_ms']
                }
    
class GetOrderDefaultRecord:
    def __init__(self):
        self.zs_source_mapping = MappingSource()
        self.full_cols = SchemaUtilsFunction().get_col_name()
        self.mapping_gateway = PaymentGateWay()
        self.change_to_datetime_fn = lambda x: datetime.fromtimestamp(x / 1000)
        self.query_info = QueryInfoFromDatabase(os.getenv('DEV_ENV'))
        
    def _init_row(self):
        return {k:None for k in self.full_cols}
    
    def _get_usertype(self, row, message):
        if message['source'] == 'temp':
            row['user_type'] = 'unknown_user'
            row['status'] = 'initiated'
            return row

        if row['customer_id'] is not None:
            row['user_type'] = 'approved_user'
            row['status'] = 'initiated'
            return row
        
        row['user_type'] = 'identified_user'
        row['status'] = 'initiated'
        return row

    def _get_platform(self, x):
        partern = """(?<=platform=).*(?=)"""
        try:
            return re.match(partern, x).group(0)
        except:
            return None

    def get_default_info_row(self, message):
        row = self._init_row()
        if message['source'] == 'temp':
            parse_json_temp_data = json.loads(message.TEMP_DATA)
            
            row['ref_order_id'] = parse_json_temp_data['order']['order_id']
            row['order_value'] = parse_json_temp_data['order']['amount']
            merchant_info = self.query_info.query_merchant_name_with_shopid(parse_json_temp_data['order']['shop_id'])
            row['lender'] = 'fundiin'

            row['merchant_id'] = merchant_info['merchant_id']
            row['merchant'] = merchant_info['merchant']

            row['created_time'] = datetime.fromtimestamp(message['payload']['tn_ms'] / 1000)

            row['zs_source'] = parse_json_temp_data['shop_type']
            row['zs_terminal'] = None
            row['zs_method'] = self._get_platform(parse_json_temp_data['callback_url'])
            
            row['item'] = [{'name': i['product_name'], 'quantity': i['quantity']} for i in parse_json_temp_data['order']['product_items']]

            row = self._get_usertype(row, message)
        
        elif message['source'] == 'pre_booking':
            info = message['after']

            row['pre_booking_id'] = info['id']
            row['order_id'] = info['booking_id']
            row['order_value'] = info['amount']
            row['merchant_id'] = info['merchantid']
            row['lender'] = 'fundiin'

            row['created_time'] = self.change_to_datetime_fn(info['created_time'] / 1000)

            row['zs_source'] = info['source']
            row['zs_terminal'] = None
            row['zs_method'] = info['platform']
            
            if info['order_detail'] is not None: 
                try:
                    row['item'] = [{'name': i['productName'], 'quantity': i['quantity']} for i in json.loads(info['order_detail'])['productItems']]
                except:
                    pass
            row['emc_item'] = info['order_info']

            row['phone_number'] = info['phone_number']
            row['customer_id'] = info['customer_id']
            # row['register_id'] = message.REGISTER_ID
            row = self._get_usertype(row, message)

        else:
            info = message['after']
            
            row['ref_order_id'] = info['orderid']
            row['order_id'] = info['id']

            query_pre_booking_id = list(self.query_info.query_pre_booking_id(info['id']))
            row['pre_booking_id'] = query_pre_booking_id[0]['id']

            row['order_value'] = info['totalvalue']
            row['merchant_id'] = info['merchantid']
            row['lender'] = 'fundiin'

            row['created_time'] = self.change_to_datetime_fn(info['receive_time'] / 1000)

            row['zs_source'] = info['source']
            row['zs_terminal'] = None
            row['zs_method'] = info['platform']
            
            if info['merchantorder'] is not None: 
                try:
                    row['ref_order_id'] = info['orderid']
                    row['item'] = [{'name': i['productName'], 'quantity': i['quantity']} for i in json.loads(info['merchantorder'])['productItems']]
                except:
                    pass
                
            row['emc_item'] = info['order_info']

            # row['phone_number'] = info['phone_number']
            row['customer_id'] = info['customerid']
            # row['register_id'] = message.REGISTER_ID
            row = self._get_usertype(row, message)
        
        return row

class ClassifyMessage:
    def __init__(self) -> None:
        self.type = {
                    'discard': 0, 
                    'initiate_temp': 1,
                    'initiate_prebooking': 2,
                    'update_temp': 3,
                    'update': 4
                    }
        self.query_info = QueryInfoFromDatabase(os.getenv('DEV_ENV'))
        
    def init_or_update(self, mess):
        if mess['source'] == 'temp':
            if 'order_id' not in mess['after']['data'] or 'GALAXYPLAY' not in mess['after']['data']:
                return 'discard'
            return 'initiate_temp'
        if (mess['source'] == 'pre_booking' and mess['operator'] == 'c'):
            temp_id = mess['after']['order_id']
            if temp_id is not None:
                result = list(self.query_info.query_mapping_to_temp())
                if result[0] is not None:
                    return 'initiate_prebooking'
                return 'update_temp'
            return 'initiate_prebooking'
        return 'update'

    def __call__(self, message, partitions) -> Any:
        return self.type[self.init_or_update(message)]

class ClassifySourceMessage:
    def prebooking_or_booking(self, mess):
        if mess['source'] == 'pre_booking':
            return 0
        return 1

    def __call__(self, message, partitions) -> Any:
        return self.prebooking_or_booking(message)

class ClassifyUpdateMessage:
    def __init__(self) -> None:
        self.class_mapping_pb = {
            # 'waiting': 1,
            'cancel_on_prebooking': 1,
            'reject_on_prebooking': 2,
            'discard' : 0
        }

        self.class_mapping_cb = {
            'discard' : 0,
            'cancel_expired': 1 ,
            'cancel_before_authourized': 2,
            'cancel_after_authourized': 3,
            'reject_before_authourized': 4,
            'reject_before_authourized_manual': 5,
            'reject_after_authourized': 6,
            'reject_after_authourized_manual': 7,
            'processing': 8,
            'authorized': 9,
            'approved': 10,
            # 'approved_manual': 11,
            'expired': 11,
        }

    # def waiting_message(self, mess):
    #     """
    #         Created records from prebooking -> back query to temp -> have record
    #     """
    #     if mess['source'] == 'pre_booking':
    #         return self.class_mapping_pb['waiting']
    #     return 0

    def canceled_message(self, mess):
        """
            Cancel classificaion:
                - cancel_on_prebooking: Updated on prebooking status in (CANCEL)
                - Cancel_expired: Updated on course_booking (after) canceltype == 3 and cancel timestamp != null
                - Cancel_before_authourized: Updated on course_booking (after) cancel timestamp != null and paymentstatus != 1
                - Cancel_after_authourized: Updated on course_booking (after) cancel timestamp != null and paymentstatus == 1
        """
        if 'status' in mess['list_col_changed'] and mess['after']['status'] in ('CANCEL'):
            return self.class_mapping_pb['cancel_on_prebooking']
        if 'canceltype' in mess['list_col_changed'] and mess['after']['canceltype'] in (3):
            return self.class_mapping_cb['cancel_expired']
        if 'canceltype' in mess['list_col_changed'] and mess['after']['canceltype'] in (1, 2, 4):
            return self.class_mapping_cb['cancel_after_authourized']
        if 'approvalstatus' in mess['list_col_changed'] and mess['after']['approvalstatus'] in (4):
            return self.class_mapping_cb['cancel_before_authourized']
        return 0

    def rejected_message(self, mess):
        """
            Reject classificaion:
                - reject_on_prebooking: Updated on prebooking status in (ACCOUNT_REJECTED, 
                                                                        EKYC_FAIL_TOO_MANY, 
                                                                        MERCHANT_MINIMUM_AMOUNT_VIOLATE)
                - reject_before_authourized: Updated on course_booking (after) Reject timestamp != null and paymentstatus != 1
                - reject_after_authourized: Updated on course_booking (after) Reject timestamp != null and paymentstatus == 1
        """
        if 'status' in mess['list_col_changed'] and mess['after']['status'] in ('MERCHANT_MINIMUM_AMOUNT_VIOLATE', 
                                'ACCOUNT_REJECTED',
                                'EKYC_FAIL_TOO_MANY'):
            return self.class_mapping_pb['cancel_on_prebooking']
        if 'approvalstatus' in mess['list_col_changed'] and mess['after']['approvalstatus'] in (0) and mess['after']['paymentstatus'] == 0:
            if 'fundiinid' in mess['list_col_changed'] and mess['after']['fundiinid'] > 0:
                return self.class_mapping_cb['reject_before_authourized_manual']
            return self.class_mapping_cb['reject_before_authourized']
        if 'approvalstatus' in mess['list_col_changed'] and mess['after']['approvalstatus'] in (0) and mess['after']['paymentstatus'] > 0:
            if 'fundiinid' in mess['list_col_changed'] and mess['after']['fundiinid'] > 0:
                return self.class_mapping_cb['reject_after_authourized_manual']
            return self.class_mapping_cb['reject_after_authourized']

        return 0

    def processing_message(self, mess):
        """
            Created records from course_booking
        """
        if mess['source'] == 'course_booking' and mess['operator'] == 'c':
            return self.class_mapping_cb['processing']
        return 0

    def authorized_message(self, mess):
        """
            Course_booking paymentstatus change from 0 to 1
        """
        if 'paymentstatus' in mess['list_col_changed'] and mess['after']['paymentstatus'] == '1':
            return self.class_mapping_cb['authorized']
        return 0

    def approved_message(self, mess):
        """
            Course_booking approvalstaus change from any to 2
        """
        if 'approvalstatus' in mess['list_col_changed'] and mess['after']['approvalstatus'] == '2':
            return self.class_mapping_cb['approved']
        return 0

    def expired_message(self, mess):
        """
            Course_booking approvalstaus change from any to 5
        """
        if 'approvalstatus' in mess['list_col_changed'] and mess['after']['approvalstatus'] == '5':
            return self.class_mapping_cb['expired']
        return 0

    def __call__(self, message, partitions) -> Any:
        if message['source'] == 'pre_booking':
            # r1 = self.waiting_message(message)
            # if r1:
            #     return r1
            r2 = self.canceled_message(message)
            if r2:
                return r2
            r3 = self.rejected_message(message)
            if r3:
                return r3
            return 0
            
        else:
            r1 = self.processing_message(message)
            if r1:
                return r1
            r2 = self.authorized_message(message)
            if r2:
                return r2
            r3 = self.approved_message(message)
            if r3:
                return r3
            r4 = self.expired_message(message)
            if r4:
                return r4
            r5 = self.canceled_message(message)
            if r5:
                return r5
            r6 = self.rejected_message(message)
            if r6:
                return r6
            return 0

class GetOrderInitStatus(DoFn):
    def __init__(self):
        self.default_record = GetOrderDefaultRecord()
        self.query_info = QueryInfoFromDatabase(os.getenv('DEV_ENV'))
    
    def _fill_info(self, element):
        return self.default_record.get_default_info_row(element)

    def process(self, element):
        row = self._fill_info(element)
        row['rank'] = 1
        row['action_by'] = 'auto'
        yield row

class GetOrderStatusWaitingAfterInitiated(GetOrderInitStatus, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'initiated'
        row['status'] = 'waiting'
        row['rank'] = 2
        row['action_by'] = 'auto'
        yield row

class GetOrderStatusCanceldPrebooking(GetOrderInitStatus, DoFn):
    """
        Note: Time stamp not provided. Using from created time.
    """
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'initiated'
        row['status'] = 'canceled'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['action_by'] = 'auto'
        row['rank'] = 3
        yield row 

class GetOrderStatusRejectedPrebooking(GetOrderInitStatus, DoFn):
    """
        Note: Time stamp not provided. Using from created time.
    """
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'waiting'
        row['status'] = 'rejected'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['reason_id'] = element['after']['status']
        row['action_by'] = 'auto'
        row['rank'] = 3
        yield row 

class GetOrderProcessing(GetOrderInitStatus, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'waiting'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['status'] = 'processing'
        row['action_by'] = 'auto'
        row['rank'] = 3
        yield row 

class GetOrderAuthorized(GetOrderInitStatus, DoFn):
    def __init__(self):
        self.order_mapping = MappingOrderType()
        self.payment_mapping = MappingPaymentType()
        return super().__init__()

    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'processing'
        row['created_time'] = datetime.fromtimestamp(element['after']['approvedtime'] / 1000)
        row['status'] = 'authorized'
        row['action_by'] = 'customer'
        row['rank'] = 4
        yield row

class GetRejectedOrderManualBeforeAuthorized(GetOrderProcessing, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['previous_status'] = 'processing'
        row['status'] = 'manual'
        row['action_by'] = 'auto'
        row['reason_id'] = self.query_info.query_manual_reject_reason()[0]['manual_content']
        row['rank'] = 5
        yield row

class GetRejectedOrderManualAfterAuthorized(GetOrderAuthorized, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'authorized'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['status'] = 'manual'
        row['action_by'] = 'auto'
        row['reason_id'] = self.query_info.query_manual_reject_reason()[0]['manual_content']
        row['rank'] = 5
        yield row

class GetOrderApproved(GetOrderAuthorized, DoFn):
    def __init__(self):
        self.query_class = GetReconciliationConfigWithMerchantId(dev_env= os.getenv('DEV_ENV'))
        return super().__init__()
    
    def process(self, element):
        self.list_col_config = [
            'field',
            'operation',
            'operation_value',
            'value',
            'effective_date',
            'expiration_date',
            'created_date',
            'updated_date',
            'is_deleted'
            # 'label_id'
            ]
        list_col_config = ['recon_config_' + i for i in self.list_col_config]
            
        row = super()._fill_info(element)

        reconciliation_config = self.query_class.query(row['merchant_id'])
        reconciliation_config = reconciliation_config[0]

        struct_config = []
        for row in reconciliation_config:
            struct_config.append({o_key: row[key] for key, o_key in zip(list_col_config, self.list_col_config)})
        list_col_default = list(reconciliation_config[0].keys())
        remain_col = set(list_col_default) - set(self.list_col_config)
        add_on_config = {col: reconciliation_config[0][col] for col in remain_col}
        add_on_config.update({'reconciliation_config':struct_config})
        row.update(add_on_config)

        row['previous_status'] = 'authorized' if element['after']['fundiinid'] == 0 else 'manual'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['status'] = 'approved'
        row['action_by'] = 'auto' if element['after']['fundiinid'] == 0 else 'manual'
        row['rank'] = 6
        yield row

class GetOrderExpired(GetOrderInitStatus, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'processing'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['status'] = 'expired'
        row['action_by'] = 'auto'
        row['rank'] = 4
        yield row

class GetOrderExpiredCanceled(GetOrderExpired, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'expired'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['status'] = 'canceled'
        row['action_by'] = 'auto'
        row['rank'] = 5
        yield row

class GetOrderCanceledWhileProcess(GetOrderProcessing, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'processing'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['status'] = 'canceled'
        row['action_by'] = 'auto'
        row['rank'] = 5
        yield row

class GetOrderCanceledRefunded(GetOrderApproved, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'approved'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['status'] = 'canceled'
        row['action_by'] = 'auto'
        row['rank'] = 5
        yield row

class GetOrderRejectedRefunded(GetOrderAuthorized, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'authorized' if element['after']['fundiinid'] == 0 else 'manual'
        row['created_time'] = datetime.fromtimestamp(element['event_time'] / 1000)
        row['status'] = 'rejected'
        row['action_by'] = 'auto' if element['after']['fundiinid'] == 0 else 'manual'
        row['reason_id'] = self.query_info.query_manual_reject_reason()[0]['content']
        row['rank'] = 5
        yield row

class GetOrderRejectedBeforeAuth(GetOrderProcessing, DoFn):
    def __init__(self):
        return super().__init__()
    
    def process(self, element):
        row = super()._fill_info(element)
        row['previous_status'] = 'authorized' if element['after']['fundiinid'] == 0 else 'manual'
        row['created_time'] = datetime.fromtimestamp(element.BOOKING_CREATED_TIME / 1000)
        row['status'] = 'rejected'
        row['action_by'] = 'auto' if row['previous_status'] == 'manual' else 'manual'
        row['reason_id'] = self.query_info.query_manual_reject_reason()[0]['content']
        row['rank'] = 5
        yield row

def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--job_name',
        dest='job_name',
        required=False,
        default='order-history',
        help='Job name.')

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
    
    parser.add_argument(
        '--output',
        default='bq_table',
        help='Big query table to write to')
    
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
    # job_name = known_args.job_name + '-' + str(round(datetime.now().timestamp() * 1000))
    # pipeline_args.append('--job_name=' + job_name)

    # GLOBAL ENV
    cdc_message_parse = CDCMessageParse()

    p = Pipeline(options=PipelineOptions(pipeline_args))
    _, \
    initiate_temp,\
    initiate_prebooking,\
    update_temp,\
    update = (
        p
        | "Read message" >> ReadFromKafka(
            consumer_config={'bootstrap.servers': known_args.bootstrap_servers},
            topics= known_args.topics,
            # topics= ['order_history.fundiin_api_uat_02.pre_booking'],
            with_metadata=known_args.with_metadata,
            max_num_records = 5,
            start_read_time = 1702262400000
            )

        | "Byte to dict" >> beam.Map(lambda record: cdc_message_parse.convert_kafka_record_to_dictionary(record))

        | "Filter message by operation" >> beam.Filter(lambda record: cdc_message_parse.filter_op(record))

        | "Parse message" >> beam.Map(lambda record: cdc_message_parse.get_change_column_values(record))

        | "Classify message" >> Partition(ClassifyMessage(), 5)
        )

    initiate_row = (
        initiate_prebooking
        | "Get initiate row" >> ParDo(GetOrderInitStatus())
    )

    initiate_row_temp = (
        initiate_temp
        | "Get initiate row from temp" >> ParDo(GetOrderInitStatus())
    )

    order_waiting = (
        [
            update_temp,
            initiate_prebooking
        ]
        | beam.Flatten()
        | 'Get waiting status' >> ParDo(GetOrderStatusWaitingAfterInitiated())
    )

    prebooking_update, booking_update = (
        update
        | "Split update message (source)" >> Partition(ClassifySourceMessage(), 2)
    )

    _, \
    cancel_on_prebooking, \
    reject_on_prebooking = (prebooking_update 
        | "Split update message (status + prebooking)" >> Partition(ClassifyUpdateMessage(), 3)
    )

    order_canceled_prebooking = (cancel_on_prebooking
        | 'Get canceled prebooking status' >> ParDo(GetOrderStatusCanceldPrebooking())
    )

    order_rejected_prebooking = (reject_on_prebooking
        | 'Get rejected prebooking status' >> ParDo(GetOrderStatusRejectedPrebooking())
    )

    _,\
    cancel_expired,\
    cancel_before_authourized,\
    cancel_after_authourized,\
    reject_before_authourized,\
    reject_before_authourized_manual,\
    reject_after_authourized,\
    reject_after_authourized_manual,\
    processing,\
    authorized,\
    approved,\
    expired = (booking_update 
        | "Split update message (booking + status)" >> Partition(ClassifyUpdateMessage(), 12)
        
    )

    order_processing = (processing
        | 'Get processing status' >> ParDo(GetOrderProcessing())
    )

    order_authorized = (authorized
        | 'Get authorized status' >> ParDo(GetOrderAuthorized())
    )

    order_expired = (expired
        | 'Get expired status' >> ParDo(GetOrderExpired())
    )

    order_manual_reject_after_auth = (reject_after_authourized_manual
        | 'Get manual after auth status' >> ParDo(GetRejectedOrderManualAfterAuthorized())
    )

    order_manual_reject_before_auth = (reject_before_authourized_manual
        | 'Get manual before auth status' >> ParDo(GetRejectedOrderManualBeforeAuthorized())
    )

    order_approved = (approved
        | 'Get approved status' >> ParDo(GetOrderApproved())
    )

    order_canceled_on_expired = (cancel_expired
        | 'Get canceled on expired' >> ParDo(GetOrderExpiredCanceled())
    )


    order_canceled_while_process = (cancel_before_authourized
        | 'Get canceled while processing status' >> ParDo(GetOrderCanceledWhileProcess())
    )

    order_canceled_refunded = (cancel_after_authourized
        | 'Get canceled status' >> ParDo(GetOrderCanceledRefunded())
    )

    order_rejected_refunded = (reject_after_authourized
        | 'Get rejected after auth status' >> ParDo(GetOrderRejectedRefunded())
    )


    order_rejected_before_auth = (reject_before_authourized
        | 'Get rejected before auth status' >> ParDo(GetOrderRejectedBeforeAuth())
    )
    
    (
        [
        initiate_row_temp,
        initiate_row, 
        order_waiting, 
        order_canceled_prebooking, 
        order_rejected_prebooking,
        order_processing,
        order_authorized,
        order_approved,
        order_expired,
        order_canceled_on_expired,
        order_canceled_refunded,
        order_canceled_while_process,
        order_rejected_refunded,
        order_rejected_before_auth,
        order_manual_reject_after_auth,
        order_manual_reject_before_auth
        ]
        | 'Merge' >> beam.Flatten()
        # | beam.Map(print)
        # | beam.Filter(lambda element: element is not None)
        | 'Write to file' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output,
            # Here we use the JSON schema read in from a JSON file.
            # Specifying the schema allows the API to create the table correctly if it does not yet exist.
            schema= (SchemaUtilsFunction().parse_table_schema_from_json()),
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method='STREAMING_INSERTS')
            )
    )
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
