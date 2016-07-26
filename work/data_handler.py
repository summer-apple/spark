import pydevd
from pyspark import SparkContext,SparkConf,SQLContext

try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys,os
    sys.path.append(os.path.abspath('../'))
    from work.mysql_helper import MySQLHelper


pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)




class DataHandler:
    def __init__(self):
        self.conf = (SparkConf()
                                 .setAppName("Summer")
                                 .set("spark.cores.max", "2")
                                 .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)

        self.mysql_helper = MySQLHelper('core',host='10.9.29.212')

    def load_from_mysql(self,table,database='core'):
        url = "jdbc:mysql://10.9.29.212:3306/%s?user=root&characterEncoding=UTF-8" % database
        df = self.sqlctx.read.format("jdbc").options(url=url, dbtable=table, driver="com.mysql.jdbc.Driver").load()
        return df


    def split_date(self,line):

        l = list(line)
        l.append(line[0][:4])
        l.append(line[0][5:])

        return l

    def print_test(self,line):
        print(line)


    def life_cycle(self,year,season):

        if season == 1:
            start_year = year -1
            end_year = year
            t1_start_month = 9
            t1_end_month = 12
            t2_start_month = 1
            t2_end_month = 3

        else:
            start_year = year
            end_year = year
            t1_start_month = 3 * season -5
            t1_end_month = 3 * season -3
            t2_start_month = 3 * season -2
            t2_end_month = 3 * season



        #加载AUM表
        aum = self.load_from_mysql('t_CMMS_ASSLIB_ASSET').cache()

        month_4 = aum.filter(aum.STAT_DAT == '2016-04')
        month_5 = aum.filter(aum.STAT_DAT == '2016-05')
        month_6 = aum.filter(aum.STAT_DAT == '2016-06')


        season_2 = month_4.unionAll(month_5).unionAll(month_6)

        print(season_2.count())


        season_2.select('CUST_NO','AUM').groupBy('CUST_NO').sum('AUM').show()




if __name__ == '__main__':
    dh = DataHandler()
    dh.life_cycle(2016,2)