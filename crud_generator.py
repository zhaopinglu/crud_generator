#!/usr/bin/python3
# Purpose: Simple CRUD sql load generator for MySQL
# Created: Zhaoping Lu (zhaopinglu77@gmail.com)

# Update:
# v0.1, created, 20240110
# v0.2, Add weight property for random generator. Add repeat property for sequence generator, 20240125
# v0.3, Add 'increase_info' property, 20240129
# v0.4, Add 'output_format' property, 20240719
# v0.5, Generate column values in dedicate thread val_thread to avoid lock contention, 20240724

# Known Issues
# 1. When queue size reached maxsize, the get() call could be blocked forever. Reason unknown.

# Setup:
#   Python 3.6.8: 
#       python3 -m pip install --upgrade pip # optional
#       pip3 install python-dateutil mysql-connector-python==8.0.29
#   Python 3.7+:
# 	    pip3 install python-dateutil mysql-connector-python

# Remote debug
# pip3 install debugpy
# python -m debugpy --listen 0.0.0.0:5678 ./crud_generator.py ORG_SALES_DTL.json

import logging

logging.TRACE = logging.DEBUG - 5
# Config Begin ###########################################################
CONSOLE_LOGLEVEL = logging.INFO
FILE_LOGLEVEL = logging.TRACE
# Config End ###########################################################


from math import floor
import traceback
import concurrent.futures
import threading
import queue
import time
import sys
import json
import random
import uuid
import os
from datetime import datetime, timedelta
import mysql.connector
import re
from dateutil.relativedelta import relativedelta
from datetime import datetime
import itertools


# import debugpy
# debugpy.listen(('0.0.0.0', 5678))
# debugpy.wait_for_client()

def uuid_str_generator():
    while True:
        uuid_str = str(uuid.uuid4())
        yield uuid_str.replace('-', '')


class MultiChannelQueue:
    def __init__(self, maxsize=1024):
        self.queues = {}
        self.maxsize = maxsize

    def put(self, channel, item):
        if channel not in self.queues:
            self.queues[channel] = queue.Queue(maxsize=self.maxsize)
        self.queues[channel].put(item)

    def get(self, channel):
        if channel not in self.queues:
            self.queues[channel] = queue.Queue(maxsize=self.maxsize)
        return self.queues[channel].get()

class SQLQueue:
    def __init__(self, max_queue_size):
        self.queue = queue.PriorityQueue(maxsize=max_queue_size)

    def put_with_random_priority(self, item):
        # The underlying queue is a PriotyQueue which use '<' to compare the input full_item
        # If the 1st field is equal, it will compare the 2nd field which is a dict and don't support '<' operator
        # Thus will hit error: TypeError: '<' not supported between instances of 'dict' and 'dict'
        # Solution: put a uuid value into 1st field.
        full_item = (next(uuid_str_generator()), item)

        # Caution: This trace operation will slow down the sql generation!!!
        # logger.trace(f"put:{full_item}")

        self.queue.put(full_item)

    def get_query(self):
        logger.trace(f"queue_get_begin")
        try:
            #query = self.queue.get(timeout=3)[1]
            query = self.queue.get()[1]
        except Exception as e:
            logger.error(f"Something wrong when getting query from queue. Exception: {str(e)}")
            raise Exception(str(e))

        logger.trace(f"queue_get_end")
        return query

    def get(self):
        return self.queue.get()

    def qsize(self):
        return self.queue.qsize()


class DBConnectionManager:
    def __init__(self, conn_dict=None):
        self.conn_dict = conn_dict
        self.conn_mgr = {}

    # conn_name: 'table_connection': used for crud; 'condition_connection': used for retrieving condition values
    def get_connection(self, conn_name, conn_dict=None):
        if conn_dict is None:
            conn_dict = self.conn_dict
        if conn_dict is None:
            raise Exception(f"Missing connection information when creating connection for {conn_name}")

        if conn_name not in self.conn_mgr:
            logger.trace(f"creating db connection {conn_name}: {conn_dict}")
            conn = mysql.connector.connect(**conn_dict)
            conn.autocommit = True
            self.conn_mgr[conn_name] = conn
        return self.conn_mgr[conn_name]

    def close_connections(self):
        for conn_name, connection in self.conn_mgr:
            if connection is not None:
                connection.close()

    def __del__(self):
        self.close_connections()


class ColumnDefinition:
    def __init__(self, table_name, columns_init_info):
        self.tab_name = table_name
        self.cols_init_info = columns_init_info
        self.conn_mgr = None
        self.cols_details = {}
        self.compute_cols_details = {}
        # self.source_list_length = {}

        for col_name, col_info in self.cols_init_info.items():
            self.parse_col(col_name, col_info)

    def get_conn_mgr(self):
        if self.conn_mgr is None:
            self.conn_mgr = DBConnectionManager()
        return self.conn_mgr

    def parse_col(self, col_name, col_info):
        logger.debug('Parsing column: %s: %s', col_name, col_info)
        col_type = col_info["type"]

        if col_type == "string":
            gen = self.init_string_generator(col_name, col_info)
        elif col_type == "datetime":
            gen = self.init_datetime_generator(col_name, col_info)
        elif col_type == "number":
            gen = self.init_number_generator(col_name, col_info)
        elif col_type == "compute_column":
            gen = self.init_compute_column_generator(col_name, col_info)
        else:
            raise Exception("Unimplemented. Wrong type value: %s", col_info)

        self.store_generator(col_name, "data_generator", gen)
        self.parse_set_clause_attr(col_name, col_info)
        self.parse_where_cond(col_name, col_info)
        self.parse_inc_ratio_attr(col_name, col_info)

        # set unique_group
        if "unique_group" in col_info and col_info["type"] != "compute_column":
            # unique_group property must be used with access_method 'sequence'
            # if "access_method" not in col_info or col_info["access_method"] == "random":
            #     raise Exception(f"Error in the definition of column {col_name}: The unique_group property can not be used with access_method random")

            logger.debug(f"Building unique group dict: col_name:{col_name}, unique group: {col_info['unique_group']}")
            ug_mbr_list = self.unique_groups.get(col_info['unique_group'],[])
            ug_mbr_list.append(col_name)
            self.unique_groups[col_info['unique_group']] = ug_mbr_list



    def parse_inc_ratio_attr(self, col_name, col_info):
        self.cols_details[col_name]["cumulative_ratio"] = 1.0
        if 'increase_info' in col_info:
            self.cols_details[col_name]["increase_info"] = col_info["increase_info"]

    def parse_set_clause_attr(self, col_name, col_info):
        if "is_in_set_clause" in col_info and col_info["is_in_set_clause"] == "true":
            self.cols_details[col_name]["set_clause"] = "true"
        else:
            self.cols_details[col_name]["set_clause"] = "false"

    def parse_where_cond(self, col_name, col_info):
        if "is_in_where_clause" in col_info and col_info["is_in_where_clause"] == "true":
            where_cond_def = col_info["where_condition"]
            self.parse_cond(col_name, where_cond_def)

    def parse_cond(self, col_name, cond_def):
        logger.debug('Parsing condition: %s: %s', col_name, cond_def)
        if cond_def["access_method"] == "database" and cond_def["db_type"] == "mysql":
            gen = self.init_cond_mysql_generator(col_name, cond_def)
        # todo: add more db types if necessary.
        else:
            raise Exception(f"Unimplemented. Wrong access_method or db_type, {col_name}:: {cond_def}")

        self.store_generator(col_name, "condition_generator", gen)

    def init_cond_mysql_generator(self, col_name, cond_def):
        query = cond_def["query"]
        logger.debug(f'init mysql generator with query: {query}')

        conn = self.get_connection_for_column("condition_connection", cond_def["db_conn_info"])
        while True:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            if rows is None or len(rows) == 0:
                yield None
            else:
                for row in rows:
                    yield row

    # generator_type: 'data_generator': column value; 'condition_generator': condition value
    def store_generator(self, col_name, generator_type, generator):
        if col_name not in self.cols_details:
            self.cols_details[col_name] = {generator_type: generator}
        else:
            self.cols_details[col_name][generator_type] = generator
        logger.debug('Set generator. column: %s, generator_type: %s, generator: %s',
                     col_name, generator_type, generator)

    def get_cols(self):
        return self.cols_details

    def gen_set_clause(self):
        return ', '.join(
            [f"{col_name}={repr(next(val['data_generator']))}" for col_name, val in self.cols_details.items() if
             val["set_clause"] == "true"])

    def gen_where_clause(self):
        # return ' where 1=1 and ' + ' and '.join(
        #     [f"{col_name}={repr(next(val['condition_generator'])[col_name])}" for col_name, val in
        #      self.cols_def.items() if "condition_generator" in val])
        where_clause_parts = []
        for col_name, val in self.cols_details.items():
            if "condition_generator" in val:
                condition = next(val["condition_generator"])
                if condition is None:
                    where_clause_parts.append(f"{col_name} is null")
                else:
                    where_clause_parts.append(f"{col_name} = {repr(condition[col_name])}")
        where_clause = ' where 1=1 and ' + " and ".join(where_clause_parts)
        return where_clause

    def gen_insert(self, num_records=1):
        insert_sql = f"INSERT INTO {self.tab_name} ({', '.join(self.cols_details.keys())}) VALUES "
        col_values = {}
        while True:
            for _ in range(num_records):
                col_values = self.gen_insert_values(col_values)
                insert_sql += f"({', '.join(map(repr, col_values.values()))}), "
            insert_sql = insert_sql.rstrip(', ')
            yield insert_sql

    def gen_insert_values(self, col_values):
        global gbl_gen_lock

        # values = [next(val["data_generator"]) for val in self.cols_def.values()]

        val_dict = gbl_cnf.val_mq.get('val_dict')
        logger.trace(f"get values from mq: {val_dict}")

        # gen value from 'data_generator'
        for col_name, col_props in self.cols_details.items():
            # skip col in compute cols
            if col_name in self.compute_cols_details:
                col_values[col_name] = None
                continue

            # with gbl_gen_lock:
            #     val = next(col_props["data_generator"])
            val = val_dict.get(col_name)

            if val is None:
                logger.error(f"Error: {col_name} value is None")
                raise Exception(f"Error: {col_name} value is None")

            # Init col_values
            if col_name not in col_values:
                col_values[col_name] = val

            # If col value changed, set cumulative ratio
            if 'increase_info' in col_props and val != col_values[col_name]:
                self.set_cumulative_ratio_by(col_name)

            col_values[col_name] = val

        # ug_value_dict = next(self.unique_group_cartesian_generator())
        # for col_name, ug_val in ug_value_dict:
        #     col_values[col_name] = ug_val

        # gen value from compute_col
        for col_name, compute_col_name in self.compute_cols_details.items():
            col_values[col_name] = col_values[compute_col_name]

        return col_values

    # for now, only support 1 unique group
    # def unique_group_cartesian_generator(self):
    #     for ug_key, ug_col_list in self.unique_groups.items():
    #         col_iter_list = []
    #         for idx, col_name in enumerate(ug_col_list[::-1]):
    #             if idx == 0:
    #                 col_iter_list.append(1)
    #             else:
    #                 col_iter_list.append(col_iter_list[idx - 1] * self.source_list_length[col_name])
    #
    #
    #         # gen values for cols in unique group
    #         # col_list = []
    #         # gen_list = []
    #         result_dict = {}
    #         i=0
    #         for col_name in ug_col_list:
    #             col_gen = self.cols_def[col_name]["data_generator"]
    #             result_dict[col_name] = next(col_gen)
    #             # col_list.append(col_name)
    #             # gen_list.append( self.cols_def[col_name]["data_generator"])
    #
    #             i+=1
    #
    #
    #         # for prod in itertools.product(*gen_list):
    #         #     logger.trace(prod)
    #         #     yield dict(zip(col_list, prod))

    def set_cumulative_ratio_by(self, col_name):
        inc_cols = self.cols_details[col_name]["increase_info"]["increase_ratio"]
        for inc_col_name, inc_ratio in inc_cols.items():
            self.cols_details[inc_col_name]["cumulative_ratio"] += inc_ratio

    def gen_update(self):
        update_sql = f"UPDATE {self.tab_name} SET "
        while True:
            yield update_sql + self.gen_set_clause() + self.gen_where_clause()

    def gen_select(self):
        # select_sql = f"SELECT {', '.join(self.cols_def.keys())} FROM {self.tab_name}"
        select_sql = f"SELECT * FROM {self.tab_name}"
        while True:
            yield select_sql + self.gen_where_clause()

    def gen_delete(self):
        delete_sql = f"DELETE FROM {self.tab_name}"
        while True:
            yield delete_sql + self.gen_where_clause()

    def number_step_generator(self, begin, end, step, precision, ratio, repeat=1):
        current = begin
        if isinstance(repeat, str):
            repeat = int(repeat)

        while True:
            for _ in range(repeat):
                yield round((current * ratio), precision)

            current += step
            if current > end:
                current = begin

    def number_random_generator(self, begin, end, precision, ratio):
        while True:
            yield round(random.uniform(begin, end) * ratio, precision)

    def init_compute_column_generator(self, col_name, col_info):
        source_column = col_info["source"]
        self.compute_cols_details[col_name]=source_column
        return None


    def init_number_generator(self, col_name, col_info):
        repeat = col_info.get("repeat", 1)
        source = col_info["source"]
        begin = source["begin"]
        end = source["end"]

        if "precision" not in source:
            precision = 0
        else:
            precision = source["precision"]

        if col_name not in self.cols_details:
            cumu_ratio = 1.0
        else:
            cumu_ratio = self.cols_details[col_name]["cumulative_ratio"]

        if col_info["access_method"] == "step":
            step = source["step"]
            return self.number_step_generator(begin, end, step, precision, cumu_ratio, repeat)
        elif col_info["access_method"] == "random":
            return self.number_random_generator(begin, end, precision, cumu_ratio)
        else:
            raise Exception("Unimplemented. Wrong access_method value: %s", col_info)

    def datetime_step_generator(self, begin_date, end_date, format_str, output_format_str, step, repeat=1):
        step_units = {"ms":"milliseconds", "s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks", "M": "months", "y": "years"}

        # step_unit, step_value = step[-1], int(step[:-1])
        match = re.match(r"(\d+)([a-zA-Z]+)", step)
        step_value = int(match.group(1))
        step_unit = match.group(2)

        step_timedelta = None
        if step_unit == "M":
            step_timedelta = relativedelta(months=step_value)
        elif step_unit == "y":
            step_timedelta = relativedelta(years=step_value)
        elif step_unit in ["ms", "s", "m", "h", "d", "w"]:
            step_timedelta = timedelta(**{step_units[step_unit]: step_value})
        else:
            raise Exception(f"Wrong step value: {step}.")

        current_date = begin_date

        while True:
            ret = current_date.strftime(output_format_str)
            for _ in range(repeat):
                yield ret
            current_date += step_timedelta
            if current_date > end_date:
                current_date = begin_date

    def init_datetime_generator(self, col_name, col_info):
        repeat = col_info.get("repeat", 1)
        source = col_info["source"]
        format_str = source["format"]

        # output_format prop was introduced lately. So using get method for backward compatibiility.
        output_format_str = source.get("output_format", "%Y-%m-%d %H:%M:%S")

        begin_date = datetime.strptime(source["begin"], format_str)
        end_date = datetime.strptime(source["end"], format_str)

        if col_info["access_method"] == "step":
            step = source["step"]
            return self.datetime_step_generator(begin_date, end_date, format_str, output_format_str, step, repeat)
        else:
            raise Exception("Unimplemented. Wrong access_method value: %s", col_info)

    # def random_generator(self, col_name, source):
    #     if not isinstance(source, list):
    #         source = read_source_data(source)
    #     while True:
    #         yield random.choice(source)

    def random_generator_weighted(self, col_name, source):
        if not isinstance(source, (dict, list)):
            source = read_source_data(source)

        if isinstance(source, list):
            values = source
            weights = [1] * len(source)
        else:
            values, weights = zip(*source.items())

        total_weight = sum(weights)

        while True:
            choice = random.choices(values, weights=weights, k=1)[0]
            yield choice
    def sequence_generator(self, col_name, source, repeat=1):
        if not isinstance(source, list):
            source = read_source_data(source)
            # self.source_list_length[col_name] = len(source)

        if isinstance(repeat, str):
            repeat = int(repeat)

        i = 0
        length = len(source)
        while True:
            for _ in range(repeat):
                logger.trace(f"yielding value for column {col_name}: {source[i % length]}.")
                yield source[i % length]
            i = i + 1

    def init_string_generator(self, col_name, col_info):
        if col_info["access_method"] == "uuid":
            return uuid_str_generator()
        elif col_info["access_method"] == "random":
            # return self.random_generator(col_name, col_info["source"])
            return self.random_generator_weighted(col_name, col_info["source"])
        elif col_info["access_method"] == "sequence":
            repeat = int(col_info.get("repeat", 1))
            return self.sequence_generator(col_name, col_info["source"], repeat)
        else:
            raise Exception("Unimplemented. Wrong access_method value: %s", col_info)

    def get_connection_for_column(self, conn_name, conn_info):
        conn_mgr = self.get_conn_mgr()
        return conn_mgr.get_connection(conn_name, conn_info)


class TableDefinition:
    def __init__(self, tab_json):
        # debugpy.breakpoint()
        self.tab_json = tab_json
        self.cols_def = None
        self.local_data = threading.local()

    def get_sql_counters(self):
        if not hasattr(self.local_data, 'sql_counters'):
            self.local_data.sql_counters = {"select": 0, "insert": 0, "update": 0, "delete": 0}
            logger.trace(f"sql_counters: {self.local_data.sql_counters}")
        return self.local_data.sql_counters

    def get_cols_def(self):
        # if not hasattr(self.local_data, 'cols_def'):
        if self.cols_def is None:
            # self.local_data.cols_def = ColumnDefinition(self.tab_json["name"], self.tab_json["columns"])
            self.cols_def = ColumnDefinition(self.tab_json["name"], self.tab_json["columns"])

        # return self.local_data.cols_def
        return self.cols_def

    def get_conn_mgr(self):
        if not hasattr(self.local_data, 'conn_mgr'):
            self.local_data.conn_mgr = DBConnectionManager(self.tab_json["db_conn_info"])
        return self.local_data.conn_mgr

    def get_connection_for_table(self, conn_name):
        conn_mgr = self.get_conn_mgr()
        return conn_mgr.get_connection(conn_name)

    def get_info(self):
        return self.tab_json

    def execute_sql(self, query):
        logger.trace(f"processing query: {query}")
        # import pdb; pdb.set_trace()
        op = query['op']
        try:
            logger.trace(f"execute sql begin - SQL: {query['sql']}")
            conn = self.get_connection_for_table("data_connection")
            cursor = conn.cursor()
            cursor.execute(query['sql'])
            if op == "select":
                result = cursor.fetchall()
                logger.trace(f"Select result: {result}")
            logger.trace(f"execute sql end")
        except Exception as e:
            logger.error(f"Error executing SQL: {query['sql']}, Exception: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()

    def close_connections(self):
        conn_mgr = self.get_conn_mgr()
        conn_mgr.close_connections()

    def __del__(self):
        self.close_connections()


###################################################

def generate_log_filename():
    script_filename = os.path.basename(__file__)
    base_name, _ = os.path.splitext(script_filename)
    log_filename = f"{base_name}.log"
    return log_filename


def add_loglevel_trace():
    # Define a custom log level
    logging.addLevelName(logging.TRACE, "TRACE")

    # Define a function to log at the custom trace level
    def trace(self, message, *args, **kwargs):
        if self.isEnabledFor(logging.TRACE):
            self._log(logging.TRACE, message, args, **kwargs)

    # Add the custom trace method to the Logger class
    logging.Logger.trace = trace


class MillisecondFormatter(logging.Formatter):
    def formatTime(self, record, date_fmt=None):
        t = datetime.fromtimestamp(record.created).strftime(self.default_time_format)
        s = self.default_msec_format % (t, record.msecs)
        return s


def setup_logging(log_file=generate_log_filename(), console_loglevel=logging.INFO, file_loglevel=logging.DEBUG):
    logger = logging.getLogger(__name__)
    logger.setLevel(min(console_loglevel, file_loglevel))
    formatter = MillisecondFormatter("[%(asctime)s] [%(thread)d] [%(threadName)s] [%(levelname)s - %(funcName)s] %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(console_loglevel)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(file_loglevel)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def read_source_data(file_path):
    with open(file_path, 'r', encoding='UTF-8') as source_file:
        logger.debug(f"Reading data from {source_file}")
        return json.load(source_file)


g_lock = threading.Lock()  # Lock for g_counters
g_counters = {"select": 0, "insert": 0, "update": 0, "delete": 0}


def aggregate_metrics(sql_counters):
    global g_lock, g_counters
    logger.trace("aggregate_metrics begin")

    with g_lock:
        g_counters["select"] += sql_counters["select"]
        g_counters["insert"] += sql_counters["insert"]
        g_counters["update"] += sql_counters["update"]
        g_counters["delete"] += sql_counters["delete"]

    sql_counters["select"] = 0
    sql_counters["insert"] = 0
    sql_counters["update"] = 0
    sql_counters["delete"] = 0

    logger.trace("aggregate_metrics end")


def sql_consumer():
    time.sleep(random.uniform(0.1, 1.0))
    logger.debug("Create consumer thread")
    # debugpy.breakpoint()

    # tab_def = TableDefinition(tab_json)
    sql_counters = gbl_cnf.tab_def.get_sql_counters()
    logger.trace(f"sql_counters: {sql_counters}")
    begin = time.time()
    insert_batch=gbl_cnf.tab_json.get(f"insert_batch", 1)

    while True:
        logger.trace(f"consume query begin. qsize: {gbl_cnf.sql_queue.qsize()}")
        query = gbl_cnf.sql_queue.get_query()
        if query == {}:
            logger.trace(f"got query:{query}, break the loop and quit")
            del gbl_cnf.tab_def
            break

        logger.trace(f"got query:{query}, qsize: {gbl_cnf.sql_queue.qsize()}, now begin to execute it")
        gbl_cnf.tab_def.execute_sql(query)
        logger.trace(f"consume query end. qsize: {gbl_cnf.sql_queue.qsize()}")

        op = query['op']
        if op == "insert":
            sql_counters[op] += insert_batch
        else:
            sql_counters[op] += 1

        logger.trace(f"sql_counters - {op}:{sql_counters[op]}")

        if time.time() - begin >= 1:
            aggregate_metrics(sql_counters)
            begin = time.time()


def val_producer():
    logger.debug("Create val producer thread")
    tab_def = gbl_cnf.tab_def
    cols_def = tab_def.get_cols_def()
    cols_details = cols_def.cols_details
    compute_cols = cols_def.compute_cols_details
    val_mq = gbl_cnf.val_mq

    while True:
        tmp_val_dict = {}
        for col_name, col_props in cols_details.items():
            # skip col in compute cols
            if col_name in compute_cols:
                continue

            val = next(col_props["data_generator"])
            tmp_val_dict[col_name] = val

        val_mq.put('val_dict', tmp_val_dict)
        logger.trace(f"Put values into queue: {tmp_val_dict}")

def sql_producer():
    time.sleep(random.uniform(0.1, 1.0))
    logger.debug("Create producer thread")
    # tab_def = TableDefinition(tab_json)

    while True:
        gen_and_put_sql()


def gen_and_put_sql():
    try:
        tab_json = gbl_cnf.tab_def.get_info()
        tab_cols = gbl_cnf.tab_def.get_cols_def()
        for op in ["insert", "select", "update", "delete"]:
            # import pdb; pdb.set_trace()
            begin = time.time()

            cnt = tab_json[f"{op}_per_second"]
            insert_batch = tab_json.get(f"insert_batch",1)
            logger.trace(f"Generating {op} SQL for {cnt} times")

            for _ in range(cnt):
                gen_method = getattr(tab_cols, f"gen_{op}")
                if op == "insert":
                    query = {"op": op, "sql": next(gen_method(insert_batch))}
                else:
                    query = {"op": op, "sql": next(gen_method())}

                gbl_cnf.sql_queue.put_with_random_priority(query)
            logger.trace(f"Complete 1 round of gen&put {op} sql, ela(s): {str(time.time() - begin)}")

        time.sleep(1)
    except Exception as e:
        logger.error(f"Error when generating SQL. Exception: {str(e)}")
        traceback.print_exc()
        os._exit(3)


def stat():
    global g_counters
    begin = time.time()

    prev_cnt_s = 0
    prev_cnt_i = 0
    prev_cnt_u = 0
    prev_cnt_d = 0
    prev_cnt_a = 0

    while True:
        time.sleep(1)
        ela = time.time() - begin
        cnt_s = g_counters['select']
        cnt_i = g_counters['insert']
        cnt_u = g_counters['update']
        cnt_d = g_counters['delete']
        cnt_a = cnt_s + cnt_i + cnt_u + cnt_d

        inc_cnt_s = cnt_s - prev_cnt_s
        inc_cnt_i = cnt_i - prev_cnt_i
        inc_cnt_u = cnt_u - prev_cnt_u
        inc_cnt_d = cnt_d - prev_cnt_d
        inc_cnt_a = cnt_a - prev_cnt_a

        s = str(inc_cnt_s) + "/" + str(round(cnt_s / ela)) + "/" + str(cnt_s)
        i = str(inc_cnt_i) + "/" + str(round(cnt_i / ela)) + "/" + str(cnt_i)
        u = str(inc_cnt_u) + "/" + str(round(cnt_u / ela)) + "/" + str(cnt_u)
        d = str(inc_cnt_d) + "/" + str(round(cnt_d / ela)) + "/" + str(cnt_d)
        a = str(inc_cnt_a) + "/" + str(round(cnt_a / ela)) + "/" + str(cnt_a)

        prev_cnt_s = cnt_s
        prev_cnt_i = cnt_i
        prev_cnt_u = cnt_u
        prev_cnt_d = cnt_d
        prev_cnt_a = cnt_a

        logger.info(
            f"Ela: {floor(ela) :>4} Qsz: {gbl_cnf.sql_queue.qsize() :>4} ALL(last/avg/tot): {a :>16} S: {s :>15} I: {i :>15} U: {u :>15} D: {d :>15}")


def start_metrics_thread():
    metrics_thread = threading.Thread(target=stat, args=())
    metrics_thread.start()


def start_workers():
    px = gbl_cnf.tab_json["parallel"]

    start_metrics_thread()

    logger.info(f"Creating producers & consumers. parallel:{px}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=(px * 2 + 1)) as executor:
        val_producer_futures = [executor.submit(val_producer)]
        sql_producer_futures = [executor.submit(sql_producer) for _ in range(px)]
        sql_consumer_futures = [executor.submit(sql_consumer) for _ in range(px)]

        concurrent.futures.wait(sql_producer_futures + sql_consumer_futures + val_producer_futures)
        executor.shutdown(wait=True)  # Ensure all threads are finished before exiting

class GlobalConfig:
    def __init__(self):
        self.tab_json = self.load_tab_json()
        self.tab_def = TableDefinition(self.tab_json)

        # Call get_cols_def to initialize self.tab_cols
        # So the generator can be shared with all child threads.
        self.tab_cols = self.tab_def.get_cols_def()
        self.val_mq = MultiChannelQueue(self.tab_json["max_queue_size"]*30)
        self.sql_queue = SQLQueue(self.tab_json["max_queue_size"])

    def load_tab_json(self):
        if len(sys.argv) != 2:
            print("Usage: crud_generator.py <table.json>")
            sys.exit(1)
        file_name = sys.argv[1]

        with open(file_name, 'r') as file:
            return json.load(file)




def main():
    try:
        # tab_json = global_config.tab_json
        # tab_def = global_config.tab_def
        start_workers()

    except KeyboardInterrupt:
        os._exit(2)


# main ###############################################
add_loglevel_trace()
logger = setup_logging(console_loglevel=CONSOLE_LOGLEVEL, file_loglevel=FILE_LOGLEVEL)
gbl_gen_lock = threading.Lock()
gbl_cnf = GlobalConfig()
main()
