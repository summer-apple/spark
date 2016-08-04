import pydevd
from pyspark import SparkContext,SparkConf,SQLContext
try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys,os
    sys.path.append(os.path.abspath('../'))
    from work.mysql_helper import MySQLHelper

import datetime


pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)



class KMData:
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






    def rfm(self):
        life_cycle = self.load_from_mysql('t_CMMS_ANALYSE_LIFE').select('CUST_NO', 'LIFE_CYC')
        value = self.load_from_mysql('t_CMMS_ANALYSE_VALUE').select('CUST_NO', 'CUST_VALUE','CUST_RANK')
        loyalty = self.load_from_mysql('t_CMMS_ANALYSE_LOYALTY').select('CUST_NO', 'LOYALTY')
        rfm = loyalty.join(value, 'CUST_NO', 'left_outer').join(life_cycle, 'CUST_NO', 'left_outer').distinct()\
                       .map(lambda x: (x['CUST_NO'], x['LIFE_CYC'], x['CUST_VALUE'], x['CUST_RANK'],x['LOYALTY']))

        temp = []
        sql = "replace into t_CMMS_TEMP_KMEANS_COLUMNS(CUST_NO,LIFE_CYC,CUST_VALUE,CUST_RANK,LOYALTY) values(%s,%s,%s,%s,%s)"
        for row in rfm.collect():
            if len(temp) >= 1000:
                self.mysql_helper.executemany(sql, temp)
                temp.clear()
            temp.append(row)

            if len(temp) != 1000:
                self.mysql_helper.executemany(sql, temp)
                temp.clear()






    def idcard(self):
        '''
        get sex age and local from id card number
        :return:
        '''



        kcolums = self.load_from_mysql('t_CMMS_TEMP_KMEANS_COLUMNS').select('CUST_NO')
        cust_info = self.load_from_mysql('t_CMMS_INFO_CUSTOMER')

        j = kcolums.join(cust_info,'CUST_NO','left_outer').select('CUST_NO','CUST_ID')

        year_now = datetime.datetime.now().year



        def split_map(line):

            no = line['CUST_NO']
            id = line['CUST_ID']

            if id is not None:
                sex_flag = 2
                year = 0


                if len(id) == 21:
                    year = id[9:13]
                    sex_flag = id[-2:-1]
                elif len(line) == 18:
                    year = '19'+id[9:11]
                    sex_flag = id[-1]

                # 1 man   2 woman
                sex = int(sex_flag) % 2
                age = year_now - int(year)
                local = id[3:9]

                return age, sex, local, no

            else:
                return 0,2,'000000',no


        asl = j.map(split_map).collect()


        temp = []
        sql = "update t_CMMS_TEMP_KMEANS_COLUMNS set AGE = %s, SEX = %s, LOCAL = %s where CUST_NO = %s"
        for row in asl:
            if len(temp) >= 1000:
                self.mysql_helper.executemany(sql, temp)
                temp.clear()
            temp.append(row)

            if len(temp) != 1000:
                self.mysql_helper.executemany(sql, temp)
                temp.clear()








if __name__ == '__main__':
    kmdata = KMData()

    kmdata.idcard()