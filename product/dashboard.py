import pydevd
from pyspark import SparkContext, SparkConf, SQLContext
import datetime
from pyspark.sql.types import Row
try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os

    sys.path.append(os.path.abspath('../'))
    from product.mysql_helper import MySQLHelper

#pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)


class DataHandler:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("Dashboard")
                     .set("spark.cores.max", "2")
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)

        self.mysql_helper = MySQLHelper('core', host='10.9.29.212')

    def load_from_mysql(self, table, database='core'):
        url = "jdbc:mysql://10.9.29.212:3306/%s?user=root&characterEncoding=UTF-8" % database
        df = self.sqlctx.read.format("jdbc").options(url=url, dbtable=table, driver="com.mysql.jdbc.Driver").load()
        return df




    def api(self):
        pass