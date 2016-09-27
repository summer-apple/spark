from configparser import ConfigParser

from pyspark import SparkContext,SparkConf,SQLContext

try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os

    sys.path.append(os.path.abspath('../'))
    from product.mysql_helper import MySQLHelper

# 单例注解
def singleton(cls, *args, **kw):
    instances = {}
    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton



@singleton
class SparkSingleton(object):
    def __init__(self):

        # config
        cf = ConfigParser()
        cf.read('conf/spark.conf')

        # spark
        self.conf = (SparkConf()
                     .setAppName(cf.get('spark','app_name'))
                     .set('spark.cores.max', cf.get('spark','cores_max'))
                     .set('spark.executor.extraClassPath',cf.get('spark','extra_class_path'))
                    )
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)

        # mysql
        self.mysql_url = cf.get('db','url')
        self.mysql_driver = cf.get('db','driver')
        self.mysql_helper = MySQLHelper(cf.get('db','database'), host=cf.get('db','host'))
        print('I am a singleton...')



    def load_from_mysql(self,table):
        '''
        get dataframe from mysql
        :param table:
        :return:
        '''
        return self.sqlctx.read.format('jdbc').options(url=self.mysql_url, dbtable=table, driver=self.mysql_driver).load()




if __name__ == '__main__':
    s1 = SparkSingleton()
    s2 = SparkSingleton()

    print(id(s1),id(s2))
    s1.load_from_mysql('t_CMMS_CREDIT_STAT').show()
    s2.load_from_mysql('t_CMMS_CREDIT_LIFE').show()



