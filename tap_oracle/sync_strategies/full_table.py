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

def get_where_clauses(min_date, max_date, parts):
   microseconds_diff = int( (max_date - min_date) / timedelta(microseconds=1) )
   dates = [ min_date + timedelta(microseconds=i) for i in range(0, microseconds_diff, int(microseconds_diff / parts) ) ] + [ max_date ]
   where_clauses = []
   for i in range(len(dates) - 1):
         where_clauses.append(f" FEHO_INI BETWEEN to_timestamp('{dates[i].isoformat()}') AND to_timestamp('{dates[i+1].isoformat()}') ")
   return where_clauses

def query_thread(select_sql, connection, counter, singer, state):

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
      
   cur.close()
   connection.close()

   
def sync_table(config, stream, state, desired_columns):
   connection = orc_db.open_connection(config)
   connection.outputtypehandler = common.OutputTypeHandler

   cur = connection.cursor()
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
      #START POINT, GET MAX_MIN DATES
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
      min_date, max_date = cur.fetchall()[0]
      where_clauses = get_where_clauses(min_date, max_date, int(config['partitions'])) 
      
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
      global rows_saved
      rows_saved = 0
      
      for where in where_clauses:
          
         thread = threading.Thread(target = query_thread, args = (base_query + where + order_clause, connection, counter, singer, state), daemon = True)
         thread.start()
         threads.append(thread)
            
      for thread in threads:
         thread.join()

   state = singer.write_bookmark(state, stream.tap_stream_id, 'ORA_ROWSCN', None)
   #always send the activate version whether first run or subsequent
   singer.write_message(activate_version_message)
   cur.close()
   connection.close()
   return state
