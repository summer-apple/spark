from configparser import ConfigParser

from pyspark import SparkContext,SparkConf,SQLContext

try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os
    sys.path.append(os.path.abspath('../'))
    from product.mysql_helper import MySQLHelper

class SparkEnvirnment(object):
    def __init__(self, app_name='Spark', max_cores=2):

        # config
        cf = ConfigParser()
        cf.read('conf/spark.conf')

        # spark
        self.conf = (SparkConf()
                     .setAppName(app_name)
                     .set('spark.cores.max', max_cores)
                     .set('spark.executor.extraClassPath', cf.get('spark', 'extra_class_path'))
                    )
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)
        self.hdfs_base = cf.get('spark','hdfs_base')

        # mysql
        self.mysql_url = cf.get('db', 'url')
        self.mysql_driver = cf.get('db', 'driver')
        self.mysql_helper = MySQLHelper(cf.get('db', 'database'), host=cf.get('db', 'host'))
        print('I am not a singleton...')

    def load_from_mysql(self, table):
        '''
        get dataframe from mysql
        :param table:
        :return:
        '''
        return self.sqlctx.read.format('jdbc').options(url=self.mysql_url, dbtable=table,
                                                       driver=self.mysql_driver).load()
