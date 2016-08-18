import pydevd
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.tests import Row
try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os

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
        value = self.load_from_mysql('t_CMMS_ANALYSE_VALUE').select('CUST_NO', 'CUST_VALUE', 'CUST_RANK')
        loyalty = self.load_from_mysql('t_CMMS_ANALYSE_LOYALTY').select('CUST_NO', 'LOYALTY')
        rfm = loyalty.join(value, 'CUST_NO', 'left_outer').join(life_cycle, 'CUST_NO', 'left_outer').distinct() \
            .map(lambda x: (x['CUST_NO'], x['LIFE_CYC'], x['CUST_VALUE'], x['CUST_RANK'], x['LOYALTY']))

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

        j = kcolums.join(cust_info, 'CUST_NO', 'left_outer').select('CUST_NO', 'CUST_ID')

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
                    year = '19' + id[9:11]
                    sex_flag = id[-1]

                # 1 man   2 woman
                sex = int(sex_flag) % 2
                age = year_now - int(year)
                if age > 100:
                    age = 0
                local = id[3:9]

                return age, sex, local, no

            else:
                return 0, 2, '000000', no

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

    def aum(self):
        kcolums = self.load_from_mysql('t_CMMS_TEMP_KMEANS_COLUMNS').select('CUST_NO')
        aum = self.load_from_mysql('t_CMMS_ASSLIB_ASSTOT').select('CUST_NO', 'AUM')
        j = kcolums.join(aum, 'CUST_NO', 'left_outer').distinct()

        def m(line):
            aum = line['AUM'] if line['AUM'] is not None else 0
            return aum, line['CUST_NO']

        df = j.map(m)
        sql = "update t_CMMS_TEMP_KMEANS_COLUMNS set AUM = %s where CUST_NO = %s"
        self.sql_operate(sql, df)

    def sql_operate(self, sql, df, once_size=1000):
        '''
        批量数据库操作
        :param sql:
        :param df:
        :param once_size:
        :return:
        '''
        temp = []
        for row in df.collect():
            if len(temp) >= once_size:
                self.mysql_helper.executemany(sql, temp)
                temp.clear()
            temp.append(row)

        if len(temp) != 0:
            self.mysql_helper.executemany(sql, temp)
            temp.clear()

    def cust_info(self):
        self.rfm()
        self.idcard()
        self.aum()

    '''
        ************************************************************
    '''



    #TODO 时间跨度未设置
    def credit_card(self):

        #清空缓存表，导入新的额度数据
        init_credit_sql = "insert into t_CMMS_TEMP_KMEANS_CREDIT(ACCTNBR,CREDIT) (select XACCOUNT,CRED_LIMIT from core.ACCT)"
        self.mysql_helper.execute('truncate t_CMMS_TEMP_KMEANS_CREDIT')
        self.mysql_helper.execute(init_credit_sql)



        #信用卡交易记录
        credit_df = self.load_from_mysql('t_CMMS_CREDIT_TRAN')
        credit_df = credit_df.filter(credit_df['BILL_AMTFLAG'] == '+').cache()



        #加载额度数据
        credit_limit_df = self.load_from_mysql('t_CMMS_TEMP_KMEANS_CREDIT').select('ACCTNBR','CREDIT')



        #计算循环
        join = credit_df.groupBy('ACCTNBR').sum('BILL_AMT').join(credit_limit_df,'ACCTNBR','outer')

        cycle = join.select('ACCTNBR',join['sum(BILL_AMT)'].alias('CYCLE_AMT'),'CREDIT')

        #刷卡金额与次数
        swipe_amt = credit_df.groupBy('ACCTNBR').sum('BILL_AMT')
        swipe_times = credit_df.groupBy('ACCTNBR').count()

        # swipe_amt.show()
        # swipe_times.show()

        swipe_result = swipe_amt.join(swipe_times,'ACCTNBR','outer')

        swipe_result = swipe_result.select('ACCTNBR',swipe_result['sum(BILL_AMT)'].alias('SWIPE_AMT'),swipe_result['count'].alias('SWIPE_TIMES'))






        #取现金额与次数
        codes = [2010,2050,2060,2182,2184]
        cash = credit_df.filter(credit_df['TRANS_TYPE'] == codes.pop())
        print(type(cash))
        for c in codes:
            cash = cash.unionAll(credit_df.filter(credit_df['TRANS_TYPE'] == c))


        cash_amt = cash.groupBy('ACCTNBR').sum('BILL_AMT')
        cash_count = cash.groupBy('ACCTNBR').count()

        cash_result = cash_amt.join(cash_count,'ACCTNBR','outer')
        cash_result = cash_result.select('ACCTNBR',cash_result['sum(BILL_AMT)'].alias('CASH_AMT'),cash_result['count'].alias('CASH_TIMES'))

        #cash_amt.show()
        #cash_count.show()




        #分期付款金额与次数
        installment_df = self.load_from_mysql('MPUR_D').cache()
        im_amt = installment_df.groupBy('XACCOUNT').sum('ORIG_PURCH')
        im_times = installment_df.groupBy('XACCOUNT').count()

        im_result = im_amt.join(im_times,'XACCOUNT','outer')
        im_result = im_result.select(im_result['XACCOUNT'].alias('ACCTNBR'),im_result['sum(ORIG_PURCH)'].alias('INSTALLMENT_AMT'),im_result['count'].alias('INSTALLMENT_TIMES'))




        #汇总

        all_join = cycle.join(swipe_result,'ACCTNBR','outer').join(cash_result,'ACCTNBR','outer').join(im_result,'ACCTNBR','outer')

        all_join.show()

        def m(line):
            acctnbr = line['ACCTNBR']
            credit = line['CREDIT'] if line['CREDIT'] is not None else 0
            cycle_amt = line['CYCLE_AMT'] if line['CYCLE_AMT'] is not None else 0
            try:
                cycle_times = cycle_amt/credit
            except ZeroDivisionError:
                cycle_times = 0

            swipe_amt = line['SWIPE_AMT'] if line['SWIPE_AMT'] is not None else 0
            swipe_times = line['SWIPE_TIMES'] if line['SWIPE_TIMES'] is not None else 0
            cash_amt = line['CASH_AMT'] if line['CASH_AMT'] is not None else 0
            cash_times = line['CASH_AMT'] if line['CASH_AMT'] is not None else 0
            installment_amt = line['INSTALLMENT_AMT'] if line['INSTALLMENT_AMT'] is not None else 0
            installment_times = line['INSTALLMENT_TIMES'] if line['INSTALLMENT_TIMES'] is not None else 0



            return acctnbr,credit,cycle_amt,cycle_times,swipe_amt,swipe_times,cash_amt,cash_times,installment_amt,installment_times


        sql = "replace into t_CMMS_TEMP_KMEANS_CREDIT(ACCTNBR,CREDIT,CYCLE_AMT,CYCLE_TIMES,SWIPE_AMT,SWIPE_TIMES,CASH_AMT,CASH_TIMES,INSTALLMENT_AMT,INSTALLMENT_TIMES) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        df = all_join.map(m)

        self.sql_operate(sql,df)


if __name__ == '__main__':
    kmdata = KMData()

    kmdata.credit_card()
