import pydevd
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import Vectors
import math
import decimal
from pyspark.mllib.fpm import FPGrowth,PrefixSpan

try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os

    sys.path.append(os.path.abspath('../'))
    from work.mysql_helper import MySQLHelper


class Credit:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("KMeans")
                     .set("spark.cores.max", "2")
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)
        self.mysql_helper = MySQLHelper('core', host='10.9.29.212')
        self.base = 'hdfs://master:9000/gmc/'

    def load_from_mysql(self, table, database='core'):
        url = "jdbc:mysql://10.9.29.212:3306/%s?user=root&characterEncoding=UTF-8" % database
        df = self.sqlctx.read.format("jdbc").options(url=url, dbtable=table, driver="com.mysql.jdbc.Driver").load()
        return df


    def prepare_fpgrowth_data(self):
        tran_df = self.load_from_mysql('t_CMMS_CREDIT_TRAN').filter("BILL_AMTFLAG = '+'").select('ACCTNBR','MER_CAT_CD')\
                                                            .filter("MER_CAT_CD != 0").filter("MER_CAT_CD != 6013")

        result = tran_df.map(lambda x: (str(int(x['ACCTNBR'])),[str(int(x['MER_CAT_CD'])),])).groupByKey()

        def m(x):
            k = x[0]
            l = list(x[1])

            v = set()
            for i in l:
                v.add(i[0])

            return set(v)

        result = result.map(m)
        for i in result.take(10):
            print(i)

        model = FPGrowth.train(result, minSupport=0.05, numPartitions=10)
        result = model.freqItemsets().collect()
        for r in result:
            print(r)


if __name__ == '__main__':

    c = Credit()
    c.prepare_fpgrowth_data()