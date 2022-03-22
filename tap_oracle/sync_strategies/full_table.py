#!/usr/bin/env python3
import singer
from singer import utils, write_message, get_bookmark
import singer.metadata as metadata
from singer.schema import Schema
import tap_oracle.db as orc_db
import tap_oracle.sync_strategies.common as common
import singer.metrics as metrics
import copy
import pdb
import time
import decimal
import cx_Oracle
from datetime import datetime, timedelta
import threading

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 1000

def sync_view(config, stream, state, desired_columns):
   connection = orc_db.open_connection(config)
   connection.outputtypehandler = common.OutputTypeHandler

   cur = connection.cursor()
   cur.execute("ALTER SESSION SET TIME_ZONE = '00:00'")
   cur.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS."00+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")
   time_extracted = utils.now()

   #before writing the table version to state, check if we had one to begin with
   first_run = singer.get_bookmark(state, stream.tap_stream_id, 'version') is None

   #pick a new table version
   nascent_stream_version = int(time.time() * 1000)
   state = singer.write_bookmark(state,
                                 stream.tap_stream_id,
                                 'version',
                                 nascent_stream_version)
   singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

   # cur = connection.cursor()
   md = metadata.to_map(stream.metadata)
   schema_name = md.get(()).get('schema-name')

   escaped_columns = map(lambda c: common.prepare_columns_sql(stream, c), desired_columns)
   escaped_schema  = schema_name
   escaped_table   = stream.table
   activate_version_message = singer.ActivateVersionMessage(
      stream=stream.tap_stream_id,
      version=nascent_stream_version)

   if first_run:
      singer.write_message(activate_version_message)

   with metrics.record_counter(None) as counter:
      select_sql      = 'SELECT {} FROM {}.{}'.format(','.join(escaped_columns),
                                                      escaped_schema,
                                                      escaped_table)

      LOGGER.info("select %s", select_sql)
      for row in cur.execute(select_sql):
         record_message = common.row_to_singer_message(stream,
                                                       row,
                                                       nascent_stream_version,
                                                       desired_columns,
                                                       time_extracted)
         singer.write_message(record_message)
         counter.increment()

   #always send the activate version whether first run or subsequent
   singer.write_message(activate_version_message)
   cur.close()
   connection.close()
   return state

def where_clauses_integer(column_name, min_val, max_val, parts):
   diff = max_val - min_val
   if diff < parts:
       parts = diff
   import math
   intervals = [ min_val + i for i in range(0, diff, int(math.ceil( diff / parts) )) ] + [ max_val ]
   where_clauses = []
   for i in range(len(intervals) - 1):
         where_clauses.append(" {} >= ('{}') AND {} < ('{}') "
                              .format(column_name, intervals[i], column_name, intervals[i+1]))
   where_clauses[-1] = where_clauses[-1].replace("<","<=")
   return where_clauses

def where_clauses_datetime(column_name, min_date, max_date, parts):
   microseconds_diff = int( (max_date - min_date) / timedelta(microseconds=1) )
   dates = [ min_date + timedelta(microseconds=i) for i in range(0, microseconds_diff, int(microseconds_diff / parts) ) ] + [ max_date ]
   where_clauses = []
   for i in range(len(dates) - 1):
         where_clauses.append(" {} BETWEEN to_timestamp('{}') AND to_timestamp('{}') "
                              .format(column_name, dates[i].isoformat(), dates[i+1].isoformat()))
   return where_clauses

def query_thread(select_sql, config, counter, params):
      
   connection = orc_db.open_connection(config)
   connection.outputtypehandler = common.OutputTypeHandler
   cur = connection.cursor()
   
   cur.execute("ALTER SESSION SET TIME_ZONE = '00:00'")
   cur.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS."00+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")
   
   rows_saved = 0
   LOGGER.info("select %s", select_sql)
   for row in cur.execute(select_sql):
      ora_rowscn = row[-1]
      row = row[:-1]
      record_message = common.row_to_singer_message(params['stream'],
                                                    row,
                                                    params['nascent_stream_version'],
                                                    params['desired_columns'],
                                                    params['time_extracted'])

      singer.write_message(record_message)
      state = singer.write_bookmark(params['state'], params['stream'].tap_stream_id, 'ORA_ROWSCN', params['ora_rowscn'])
      rows_saved = rows_saved + 1
      if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
         singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

      counter.increment()
      
   cur.close()
   connection.close()
   
def partition_strategy(config, connection, stream, state, desired_columns):
   
   time_extracted = utils.now()

   #before writing the table version to state, check if we had one to begin with
   first_run = singer.get_bookmark(state, stream.tap_stream_id, 'version') is None

   #pick a new table version IFF we do not have an ORA_ROWSCN in our state
   #the presence of an ORA_ROWSCN indicates that we were interrupted last time through
   if singer.get_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN') is None:
      nascent_stream_version = int(time.time() * 1000)
   else:
      nascent_stream_version = singer.get_bookmark(state, stream.tap_stream_id, 'version')

   state = singer.write_bookmark(state,
                                 stream.tap_stream_id,
                                 'version',
                                 nascent_stream_version)
   singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

   cur = connection.cursor()
   md = metadata.to_map(stream.metadata)
   schema_name = md.get(()).get('schema-name')

   escaped_columns = map(lambda c: common.prepare_columns_sql(stream, c), desired_columns)
   escaped_schema  = schema_name
   escaped_table   = stream.table
   activate_version_message = singer.ActivateVersionMessage(
      stream=stream.tap_stream_id,
      version=nascent_stream_version)

   if first_run:
      singer.write_message(activate_version_message)
      
   with metrics.record_counter(None) as counter:
      ora_rowscn = singer.get_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN')
      if ora_rowscn:
         LOGGER.info("Resuming Full Table replication %s from ORA_ROWSCN %s", nascent_stream_version, ora_rowscn)
         select_sql      = """SELECT min({}),max({})
                                FROM {}.{}
                               WHERE ORA_ROWSCN >= {}
                               """.format(config['partition_column'],
                                          config['partition_column'],
                                           escaped_schema,
                                           escaped_table,
                                           ora_rowscn)
      else:
         select_sql      = """SELECT min({}),max({})
                                FROM {}.{}""".format(config['partition_column'],
                                           config['partition_column'],
                                           escaped_schema,
                                           escaped_table,
                                           ora_rowscn)
      cur.execute(select_sql)
      min_val, max_val = cur.fetchall()[0]
      if config["partition_column_type"] == "date-time":
         where_clauses = where_clauses_datetime(config["partition_column_type"], min_val, max_val, int(config['partitions'])) 
      else:
         where_clauses = where_clauses_integer(config["partition_column_type"], min_val, max_val, int(config['partitions'])) 
         
      if ora_rowscn:
         base_query = """SELECT {}, ORA_ROWSCN
                                FROM {}.{}
                               WHERE ORA_ROWSCN >= {} AND """.format(','.join(escaped_columns),
                                           escaped_schema,
                                           escaped_table,
                                           ora_rowscn)
      else:
         base_query = """SELECT {}, ORA_ROWSCN
                                FROM {}.{}
                                WHERE """.format(','.join(escaped_columns),
                                                                    escaped_schema,
                                                                    escaped_table)
         
      order_clause = "ORDER BY ORA_ROWSCN ASC"
      threads=[]
      params = {"counter" : counter,
                "stream": stream,
                "state" : state,
                "nascent_stream_version" : nascent_stream_version,
                "desired_columns" :  desired_columns,
                "time_extracted" : time_extracted,
                "ora_rowscn" : ora_rowscn}
      for where in where_clauses:
         thread = threading.Thread(target = query_thread, args = (base_query + where + order_clause, config, counter, params), daemon = True)
         thread.start()
         threads.append(thread)
            
      for thread in threads:
         thread.join()
         
      return state 
       
def no_partition_strategy(config, connection, stream, state, desired_columns): 

   time_extracted = utils.now()

   #before writing the table version to state, check if we had one to begin with
   first_run = singer.get_bookmark(state, stream.tap_stream_id, 'version') is None

   #pick a new table version IFF we do not have an ORA_ROWSCN in our state
   #the presence of an ORA_ROWSCN indicates that we were interrupted last time through
   if singer.get_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN') is None:
      nascent_stream_version = int(time.time() * 1000)
   else:
      nascent_stream_version = singer.get_bookmark(state, stream.tap_stream_id, 'version')

   state = singer.write_bookmark(state,
                                 stream.tap_stream_id,
                                 'version',
                                 nascent_stream_version)
   singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

   cur = connection.cursor()
   md = metadata.to_map(stream.metadata)
   schema_name = md.get(()).get('schema-name')

   escaped_columns = map(lambda c: common.prepare_columns_sql(stream, c), desired_columns)
   escaped_schema  = schema_name
   escaped_table   = stream.table
   activate_version_message = singer.ActivateVersionMessage(
      stream=stream.tap_stream_id,
      version=nascent_stream_version)

   if first_run:
      singer.write_message(activate_version_message)
      
   with metrics.record_counter(None) as counter:
      ora_rowscn = singer.get_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN')
      if ora_rowscn:
         LOGGER.info("Resuming Full Table replication %s from ORA_ROWSCN %s", nascent_stream_version, ora_rowscn)
         select_sql      = """SELECT {}, ORA_ROWSCN
                                FROM {}.{}
                               WHERE ORA_ROWSCN >= {}
                               ORDER BY ORA_ROWSCN ASC
                                """.format(','.join(escaped_columns),
                                           escaped_schema,
                                           escaped_table,
                                           ora_rowscn)
      else:
         select_sql      = """SELECT {}, ORA_ROWSCN
                                FROM {}.{}
                               ORDER BY ORA_ROWSCN ASC""".format(','.join(escaped_columns),
                                                                    escaped_schema,
                                                                    escaped_table)

      rows_saved = 0
      LOGGER.info("select %s", select_sql)
      for row in cur.execute(select_sql):
         ora_rowscn = row[-1]
         row = row[:-1]
         record_message = common.row_to_singer_message(stream,
                                                       row,
                                                       nascent_stream_version,
                                                       desired_columns,
                                                       time_extracted)

         singer.write_message(record_message)
         state = singer.write_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN', ora_rowscn)
         rows_saved = rows_saved + 1
         if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
            singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

         counter.increment()
     
   return state

def sync_table(config, stream, state, desired_columns):
   
   connection = orc_db.open_connection(config)
   connection.outputtypehandler = common.OutputTypeHandler
   
   LOGGER.info("%s", config["partition_column"])
   LOGGER.info("%s", config["partition_column_type"])
   
   if "partition_column_type" in config:
      state = partition_strategy(config, connection, stream, state, desired_columns)
   else:   
      state = no_partition_strategy(config, connection, stream, state, desired_columns)
   
   state = singer.write_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN', None)
   #always send the activate version whether first run or subsequent
   singer.write_message(activate_version_message)
   cur.close()
   connection.close()
   return state
