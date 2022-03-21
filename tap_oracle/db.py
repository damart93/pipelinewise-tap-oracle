import singer
import cx_Oracle

LOGGER = singer.get_logger()

def fully_qualified_column_name(schema, table, column):
    return '"{}"."{}"."{}"'.format(schema, table, column)

def make_dsn(config):
   return cx_Oracle.makedsn(config["host"], config["port"], service_name=config["sid"])

def open_connection(config):
    LOGGER.info("dsn: %s", config["connection_string"])
    #conn = cx_Oracle.connect(config["user"], config["password"], make_dsn(config))
    if "connection_string" in config:
        conn = cx_Oracle.connect(user="hr", password=userpwd, dsn=config["connection_string"])
    else:
        conn = cx_Oracle.connect(config["user"], config["password"], make_dsn(config))
    return conn
