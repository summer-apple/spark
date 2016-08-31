import pydevd
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import Row
from pyspark.mllib.fpm import FPGrowth

try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os

    sys.path.append(os.path.abspath('../'))
    from work.mysql_helper import MySQLHelper

#pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)

class Credit:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("CREDIT")
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

    def sql_operate(self, sql, rdd, once_size=1000):
        temp = []
        for row in rdd.collect():
            # print(row)
            if len(temp) >= once_size:
                self.mysql_helper.executemany(sql, temp)
                temp.clear()
            temp.append(row)

        if len(temp) != 0:
            self.mysql_helper.executemany(sql, temp)
            temp.clear()

    def prepare_fpgrowth_data(self):
        tran_df = self.load_from_mysql('t_CMMS_CREDIT_TRAN').filter("BILL_AMTFLAG = '+'").select('ACCTNBR',
                                                                                                 'MER_CAT_CD') \
            .filter("MER_CAT_CD != 0").filter("MER_CAT_CD != 6013")

        result = tran_df.map(lambda x: (str(int(x['ACCTNBR'])), [str(int(x['MER_CAT_CD'])), ])).groupByKey()

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

    def cycle_credit(self):
        '''
        信用卡聚类数据预处理
        :return:
        '''

        print('---------------------------信用卡-Start--------------------------')
        # 交易流水
        credit_tran_df = self.load_from_mysql('t_CMMS_CREDIT_TRAN').select('ACCTNBR', 'MONTH_NBR', 'BILL_AMT',
                                                                           'BILL_AMTFLAG').filter(
            "BILL_AMTFLAG ='-'").cache()

        # 卡账户信息
        credit_acct_df = self.load_from_mysql('ACCT_D').select('ACCTNBR', 'MONTH_NBR', 'STM_MINDUE')

        # 还款计算
        return_amt = credit_tran_df.groupBy('ACCTNBR', 'MONTH_NBR').sum('BILL_AMT')
        return_amt = return_amt.select('ACCTNBR', 'MONTH_NBR', return_amt['sum(BILL_AMT)'].alias('RETURNED'))

        # 去除0最低还款额，即未消费的账单月
        join = credit_acct_df.join(return_amt, ['ACCTNBR', 'MONTH_NBR'], 'outer').filter('STM_MINDUE != 0')

        # 清除缓存
        self.sqlctx.clearCache()

        def which_cycle_type(line):
            mindue = line['STM_MINDUE']
            returned = line['RETURNED']

            '''
            0:normal,all returned
            1:cycle credit
            2:overdue,don't return money
            '''
            if mindue is not None and returned is None:
                flag = 2
            elif returned >= mindue * 10:
                flag = 0
            elif returned > mindue and returned < mindue * 10:
                flag = 1
            else:
                flag = 9

            return Row(ACCTNBR=int(line['ACCTNBR']), MONTH_NBR=line['MONTH_NBR'], DUE_FLAG=flag,
                       STM_MINDUE=line['STM_MINDUE'])

        # 返回为PipelinedRDD
        join = join.map(which_cycle_type)

        # 转为DataFrame
        join = self.sqlctx.createDataFrame(join)
        '''
        +-------+--------+-----+
        | ACCTNBR | DUE_FLAG | count |
        +-------+--------+-----+
        | 608126 |    2 |    1 |
        | 608126 |    0 |    6 |
        | 608868 |    0 |    4 |
        '''
        # 按还款类型分类
        each_type = join.groupBy(['ACCTNBR', 'DUE_FLAG'])

        # 计算每种还款情况数量
        each_type_count = each_type.count()

        # 计算每种还款情况的最低还款额之和
        each_type_mindue_sum = each_type.sum('STM_MINDUE')

        # 计算还款情况总数
        all_type_count = each_type_count.groupBy('ACCTNBR').sum('count')

        # join 上述三表
        rate = each_type_count.join(each_type_mindue_sum, ['ACCTNBR', 'DUE_FLAG'], 'outer').join(all_type_count,
                                                                                                 'ACCTNBR', 'outer')

        # print(rate.columns)
        # ['ACCTNBR', 'DUE_FLAG', 'count', 'sum(STM_MINDUE)', 'sum(count)']

        # 筛选出循环信用的数据
        # TODO 暂时只取了循环信用的

        rate = rate.filter(rate['DUE_FLAG'] == 1)

        # 计算进入循环信用的比例
        rate = rate.select('ACCTNBR',
                           (rate['sum(STM_MINDUE)'] * 10).alias('CYCLE_AMT'),
                           rate['count'].alias('CYCLE_TIMES'),
                           (rate['count'] / rate['sum(count)']).alias('CYCLE_RATE'))

        # rate.show()
        # print(rate.count())


        def m(line):
            return line['CYCLE_TIMES'], line['CYCLE_AMT'], line['CYCLE_RATE'], line['ACCTNBR']

        sql = "update t_CMMS_TEMP_KMEANS_CREDIT set CYCLE_TIMES=%s,CYCLE_AMT=%s,CYCLE_RATE=%s where ACCTNBR=%s"

        df = rate.map(m)

        print('将数据更新至数据库...')
        self.sql_operate(sql, df)

        # 将未进入循环的 设为0
        print('将未进入循环的 设为0...')
        self.mysql_helper.execute(
            "update t_CMMS_TEMP_KMEANS_CREDIT set CYCLE_TIMES=0,CYCLE_AMT=0,CYCLE_RATE=0 where CYCLE_TIMES is null ")

    def losing_warn(self,year,month):



        # #计算月份
        # if season == 1:
        #     months_now = [1,2,3]
        #     months_before = [10,11,12]
        #     year_before = year - 1
        # else:
        #     months_now = [season * 3 - 2, season * 3 - 1, season * 3]
        #     months_before = [season * 3 - 5, season * 3 - 4, season * 3-3]
        #     year_before = year
        #
        #
        #
        # # 抽取每个季度数据
        #
        # for m in months_now:

        # 最近一个月
        month = '%02d' % month


        for i in range(2,29):
            day = '%02d' % i
            date = str(year) + month + day
            print(date)

            sql = "select MONTH_NBR from t_CMMS_CREDIT_TRAN where INP_DATE = %s limit 1"

            try:
                month_nbr = self.mysql_helper.fetchone(sql,(date,))
                if month_nbr is None:
                    continue
                else:
                    month_nbr = int(month_nbr[0])
                    break
            except Exception:
                continue

        if month_nbr is None:
            raise Exception("There is no data in database for month:%s" % month)
        else:
            print('the latest month_nbr is %s' % month_nbr)


        months_now = [month_nbr-2,month_nbr-1,month_nbr]
        months_before = [month_nbr-5,month_nbr-4,month_nbr-3]

        print('months_now',months_now)
        print('months_before',months_before)


        # 交易流水
        credit_tran_df = self.load_from_mysql('t_CMMS_CREDIT_TRAN').select('ACCTNBR', 'INP_DATE', 'MONTH_NBR', 'BILL_AMT',
                                                                           'BILL_AMTFLAG').filter("BILL_AMTFLAG ='+'").cache()



        # 筛选出两个季度对应的流水

        months_now_filter = None
        months_before_filter = None

        for i in months_now:
            f = credit_tran_df.filter(credit_tran_df['MONTH_NBR'] == i)
            if months_now_filter is None:
                months_now_filter = f
            else:
                months_now_filter = months_now_filter.unionAll(f)

        for i in months_before:
            f = credit_tran_df.filter(credit_tran_df['MONTH_NBR'] == i)
            if months_before_filter is None:
                months_before_filter = f
            else:
                months_before_filter = months_before_filter.unionAll(f)


        months_now_filter.groupBy('MONTH_NBR').count().show()
        months_before_filter.groupBy('MONTH_NBR').count().show()


        months_now_count = months_now_filter.groupBy('ACCTNBR').count()
        months_before_count = months_before_filter.groupBy('ACCTNBR').count()

        months_now_count.show()
        months_before_count.show()


        join = months_now_count.select('ACCTNBR',months_now_count['count'].alias('NOW_COUNT')).join(
            months_before_count.select('ACCTNBR',months_before_count['count'].alias('BEFORE_COUNT')),'ACCTNBR','outer'
        )

        join.show()

        def m(line):
            ncount = line['NOW_COUNT']
            bcount = line['BEFORE_COUNT']
            '''
            计算增长率
            9999：两季度均无数据
            8888：仅第一季度有数据，流失客户
            7777：仅第二季度有数据，新增客户
            其他数字：第二个季度较第一季度增长率 (s2-s1)/s1*100
            '''
            if ncount is None:
                if bcount is None:# n none,b none
                    pass
                else:# n none, b not none
                    increment = -9999
            else:
                if bcount is None:# n not none, b none
                    increment = 8888
                else:# n not none,b not none
                    increment = round((ncount-bcount)/bcount*100)

            '''
            计算信用卡生命周期（以增长率计算）
            100+ 快速增长
            50-100 增长
            -50-50 稳定
            -50- 衰退
            9999 流失

            '''
            if increment > 100 and increment != -9999:
                life = 1  # fast growing
            elif increment <=100 and increment >50:
                life = 2  # growing
            elif increment <= 50 and increment > -50:
                life = 3  # stable
            elif increment <= -50:
                life = 4  # losing
            else:
                life = 9  # no more tran  completely lost

            return line['ACCTNBR'],month_nbr,increment,life




        sql = "replace into t_CMMS_ANALYSE_CREDIT(ACCTNBR, MONTH_NBR, INCREMENT,LIFE, UPDATE_TIME) values(%s,%s,%s,%s,now())"

        rdd = join.map(m)
        print(type(rdd))

        self.sql_operate(sql,rdd)



if __name__ == '__main__':
    c = Credit()
    c.losing_warn(2016,7)
