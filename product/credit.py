import pydevd
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import Row
from pyspark.mllib.fpm import FPGrowth
from pyspark.sql import DataFrameWriter

try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os

    sys.path.append(os.path.abspath('../'))
    from product.mysql_helper import MySQLHelper

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







    def fpgrowth(self):
        '''
        用户消费商户类型频繁项
        :return:
        '''

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
        single=[]
        many=[]

        for r in result:
            if len(r[0]) == 1:
                single.append(r)
            else:
                many.append(r)

        for i in single:
            print(i[0])

        for i in many:
            print(i[0])


    def tran_stat(self, year, half_year):
        '''
        交易类型分类统计
        每种交易类型的次数和金额

        刷卡swipe、取现、分期（循环信用放在cycle_credit函数中)
        每半年统计一次
        :return:
        '''



        # 清空缓存表，导入新的额度数据
        init_credit_sql = "replace into t_CMMS_CREDIT_STAT(ACCTNBR,CREDIT) (select XACCOUNT,CRED_LIMIT from core.ACCT)"
        self.mysql_helper.execute('truncate t_CMMS_CREDIT_STAT')
        self.mysql_helper.execute(init_credit_sql)



        # 信用卡交易记录
        credit_df_orign = self.load_from_mysql('t_CMMS_CREDIT_TRAN')
        credit_df_orign = credit_df_orign.filter(credit_df_orign['BILL_AMTFLAG'] == '+').cache()


        month = 6 if half_year == 0 else 12
        month_nbr = self.get_monthnbr(year, month)
        months = list(range(month_nbr - 5, month_nbr + 1))


        # 筛选出每个月的刷卡数据，然后联合六个月的
        credit_df = credit_df_orign.filter(credit_df_orign['ACCTNBR'] == months.pop())
        for m in months:
            credit_df = credit_df_orign.unionAll(credit_df.filter(credit_df['ACCTNBR'] == m))





        # 取现交易代码
        codes = [2010, 2050, 2060, 2182, 2184]

        # 计算循环(此处为刷卡金额占额度的比例，不是真正的循环信用）
        # join = credit_df.groupBy('ACCTNBR').sum('BILL_AMT').join(credit_limit_df,'ACCTNBR','outer')
        #
        # cycle = join.select('ACCTNBR',join['sum(BILL_AMT)'].alias('CYCLE_AMT'),'CREDIT')

        # 刷卡金额与次数×××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××

        trans_without_cash = None
        for c in codes:  # 去除取现的，不然就会重复计算
            if trans_without_cash is None:
                trans_without_cash = credit_df.filter(credit_df.TRANS_TYPE != c)
            else:
                trans_without_cash = trans_without_cash.filter(credit_df.TRANS_TYPE != c)

        swipe_amt = trans_without_cash.groupBy('ACCTNBR').sum('BILL_AMT')
        swipe_times = trans_without_cash.groupBy('ACCTNBR').count()

        # swipe_amt.show()
        # swipe_times.show()

        swipe_result = swipe_amt.join(swipe_times, 'ACCTNBR', 'outer')

        swipe_result = swipe_result.select('ACCTNBR', swipe_result['sum(BILL_AMT)'].alias('SWIPE_AMT'),
                                           swipe_result['count'].alias('SWIPE_TIMES'))

        # 取现金额与次数×××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××

        cash = credit_df.filter(credit_df['TRANS_TYPE'] == codes.pop())
        print(type(cash))
        # 遍历取消交易的代码

        for c in codes:
            cash = cash.unionAll(credit_df.filter(credit_df['TRANS_TYPE'] == c))

        cash_amt = cash.groupBy('ACCTNBR').sum('BILL_AMT')
        cash_count = cash.groupBy('ACCTNBR').count()

        cash_result = cash_amt.join(cash_count, 'ACCTNBR', 'outer')
        cash_result = cash_result.select('ACCTNBR', cash_result['sum(BILL_AMT)'].alias('CASH_AMT'),
                                         cash_result['count'].alias('CASH_TIMES'))

        # cash_amt.show()
        # cash_count.show()




        # 分期付款金额与次数×××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××

        installment_df = self.load_from_mysql('MPUR_D').cache()
        im_amt = installment_df.groupBy('XACCOUNT').sum('ORIG_PURCH')
        im_times = installment_df.groupBy('XACCOUNT').count()

        im_result = im_amt.join(im_times, 'XACCOUNT', 'outer')
        im_result = im_result.select(im_result['XACCOUNT'].alias('ACCTNBR'),
                                     im_result['sum(ORIG_PURCH)'].alias('INSTALLMENT_AMT'),
                                     im_result['count'].alias('INSTALLMENT_TIMES'))

        # 汇总

        # 加载额度数据
        credit_limit_df = self.load_from_mysql('t_CMMS_CREDIT_STAT').select('ACCTNBR', 'CREDIT')

        all_join = swipe_result.join(cash_result, 'ACCTNBR', 'outer').join(im_result, 'ACCTNBR', 'outer').join(
            credit_limit_df, 'ACCTNBR', 'outer')

        all_join.show()

        def m(line):
            acctnbr = line['ACCTNBR']
            credit = line['CREDIT'] if line['CREDIT'] is not None else 0
            # cycle_amt = line['CYCLE_AMT'] if line['CYCLE_AMT'] is not None else 0
            # try:
            #     cycle_times = cycle_amt/credit
            # except ZeroDivisionError:
            #     cycle_times = 0

            swipe_amt = line['SWIPE_AMT'] if line['SWIPE_AMT'] is not None else 0
            swipe_times = line['SWIPE_TIMES'] if line['SWIPE_TIMES'] is not None else 0
            cash_amt = line['CASH_AMT'] if line['CASH_AMT'] is not None else 0
            cash_times = line['CASH_TIMES'] if line['CASH_TIMES'] is not None else 0
            installment_amt = line['INSTALLMENT_AMT'] if line['INSTALLMENT_AMT'] is not None else 0
            installment_times = line['INSTALLMENT_TIMES'] if line['INSTALLMENT_TIMES'] is not None else 0

            return acctnbr, credit, swipe_amt, swipe_times, cash_amt, cash_times, installment_amt, installment_times

        sql = "replace into t_CMMS_CREDIT_STAT(ACCTNBR,CREDIT,SWIPE_AMT,SWIPE_TIMES,CASH_AMT,CASH_TIMES,INSTALLMENT_AMT,INSTALLMENT_TIMES) values(%s,%s,%s,%s,%s,%s,%s,%s)"
        df = all_join.map(m)

        self.mysql_helper.batch_operate(sql, df)



    def cycle_credit(self, year, half_year):
        '''
        循环信用 ！!! not life_cycle
        根据还款金额时候等于最低还款额来判断
        :return:
        '''



        print('---------------------------信用卡-Start--------------------------')
        # 交易流水 还款
        credit_tran_df_orign = self.load_from_mysql('t_CMMS_CREDIT_TRAN').select('ACCTNBR', 'MONTH_NBR', 'BILL_AMT',
                                                                           'BILL_AMTFLAG').filter("BILL_AMTFLAG ='-'").cache()

        month = 6 if half_year == 0 else 12
        month_nbr = self.get_monthnbr(year, month)
        months = list(range(month_nbr - 5, month_nbr + 1))

        # 筛选出每个月的刷卡数据，然后联合六个月的
        credit_df = credit_tran_df_orign.filter(credit_tran_df_orign['ACCTNBR'] == months.pop())
        for m in months:
            credit_tran_df = credit_tran_df_orign.unionAll(credit_df.filter(credit_df['ACCTNBR'] == m))





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
            # 最低还款额
            mindue = line['STM_MINDUE']
            # 还款金额
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

        sql = "update t_CMMS_CREDIT_STAT set CYCLE_TIMES=%s,CYCLE_AMT=%s,CYCLE_RATE=%s where ACCTNBR=%s"

        rdd = rate.map(m)

        print('将数据更新至数据库...')
        self.mysql_helper.batch_operate(sql,rdd)

        # 将未进入循环的 设为0
        print('将未进入循环的 设为0...')
        self.mysql_helper.execute(
            "update t_CMMS_CREDIT_STAT set CYCLE_TIMES=0,CYCLE_AMT=0,CYCLE_RATE=0 where CYCLE_TIMES is null ")


    def life_cycle(self,year,month):
        # 信用卡生命周期
        # 根据前三月刷卡次数与后三月刷卡次数变化比例来计算

        month_nbr = self.get_monthnbr(year,month)

        # 最近三个月账单月号码
        months_now = [month_nbr-2,month_nbr-1,month_nbr]
        # 最近第四到六月账单
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


        # months_now_filter.groupBy('MONTH_NBR').count().show()
        # months_before_filter.groupBy('MONTH_NBR').count().show()
        '''
        上面两行代码用于查看各月份刷卡次数是否大致相等
        +---------+-----+
        |MONTH_NBR|count|
        +---------+-----+
        |      318|29207|
        |      319|28299|
        |      320|18346|
        +---------+-----+

        +---------+-----+
        |MONTH_NBR|count|
        +---------+-----+
        |      315|16764|
        |      316|27308|
        |      317|26141|
        +---------+-----+
        '''


        # 统计每个用户本季度和前季度的刷卡总和
        months_now_count = months_now_filter.groupBy('ACCTNBR').count()
        months_before_count = months_before_filter.groupBy('ACCTNBR').count()


        # months_now_count.show()
        # months_before_count.show()


        # 连接两季度数据
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




        sql = "replace into t_CMMS_CREDIT_LIFE(ACCTNBR, MONTH_NBR, INCREMENT,LIFE, UPDATE_TIME) values(%s,%s,%s,%s,now())"

        rdd = join.map(m)
        print(type(rdd))

        self.mysql_helper.batch_operate(sql,rdd)


    def get_monthnbr(self, year, month):
        # 最近一个月月份 %02d  用零填充
        month = '%02d' % month

        for i in range(2, 29):
            day = '%02d' % i
            date = str(year) + month + day
            print(date)

            sql = "select MONTH_NBR from t_CMMS_CREDIT_TRAN where INP_DATE = %s limit 1"

            try:
                month_nbr = self.mysql_helper.fetchone(sql, (date,))
                if month_nbr is None:
                    continue
                else:
                    return int(month_nbr[0])
                    break
            except Exception:
                continue

        if month_nbr is None:
            raise Exception("There is no data in database for month:%s" % month)
        else:
            print('month_nbr of %s %s is %s' % (year, month, month_nbr))


    def credit_loyalty(self,year,half_year):

        month = 6 if half_year == 0 else 12

        month_nbr = self.get_monthnbr(year,month)

        # 半年内各个账单月月份
        months = list(range(month_nbr-5,month_nbr+1))

        half_year_data = None

        # 交易流水 仅含消费 不含还款
        credit_tran_df = self.load_from_mysql('t_CMMS_CREDIT_TRAN').select('ACCTNBR', 'INP_DATE', 'MONTH_NBR',
                                                                           'BILL_AMT',
                                                                           'BILL_AMTFLAG').filter("BILL_AMTFLAG ='+'").cache()

        # 联合六个月的交易数据
        for i in months:
            f = credit_tran_df.filter(credit_tran_df['MONTH_NBR'] == i)
            if half_year_data is None:
                half_year_data = f
            else:
                half_year_data = half_year_data.unionAll(f)


        # 计算每个用户半年刷卡数量
        half_year_data_count = half_year_data.groupBy('ACCTNBR').count()

        half_year_data_count.show()

        # print(half_year_data_count.count())
        # print(half_year_data_count.filter("count > 20").count())
        # print(half_year_data_count.filter("count <= 20").count())
        # print(half_year_data_count.filter("count <= 10").count())
        # print(half_year_data_count.filter("count <= 5").count())

        # 根据半年刷卡总次数划分忠诚度
        # [0,5]：0 低
        # (5,10]：1 中
        # (10,20]:2 高
        # (20,++):3 极高
        def m(line):
            acctnbr = line['ACCTNBR']
            count = line['count']

            if count <= 5:
                loyalty = 0
            elif count <= 10:
                loyalty = 1
            elif count <= 20:
                loyalty = 2
            else:
                loyalty = 3

            return acctnbr,count,loyalty

        sql = "replace into t_CMMS_CREDIT_LOYALTY(ACCTNBR,USE_COUNT,LOYALTY,UPDATE_TIME ) values(%s,%s,%s,now())"

        rdd = half_year_data_count.map(m)

        self.mysql_helper.batch_operate(sql,rdd)


    def credit_value(self):

        # 每种消费类型的记分比例，贡献度Point=金额×比例
        cycle_rate = 2
        installment_rate = 0.8
        swipe_rate = 2
        cash_rate = 2

        # 获取信用卡统计的相关数据，筛选出金额字段
        credit_stat_df = self.load_from_mysql('t_CMMS_CREDIT_STAT').select('ACCTNBR','CYCLE_AMT','INSTALLMENT_AMT','SWIPE_AMT','CASH_AMT')

        # 计算贡献度
        credit_stat_df = credit_stat_df.select('ACCTNBR',(credit_stat_df['CYCLE_AMT'] * cycle_rate +
                                                        credit_stat_df['INSTALLMENT_AMT'] * installment_rate +
                                                        credit_stat_df['SWIPE_AMT'] * swipe_rate +
                                                        credit_stat_df['CASH_AMT'] * cash_rate).alias('POINT'))

        def m(line):
            '''
            0:贡献度为0   5380
            1:贡献度小于25000  3287
            2:贡献度25000-75000   2658
            3:贡献度大于75000   3422

            根据得分计算用户价值等级
            '''
            point = line['POINT']
            if point == 0:
                value = 0
            elif point <= 25000:
                value = 1
            elif point <= 75000:
                value = 2
            else:
                value = 3

            return line['ACCTNBR'], point, value


        rdd = credit_stat_df.map(m)
        sql = "replace into t_CMMS_CREDIT_VALUE(ACCTNBR,POINT,CUST_VALUE,UPDATE_TIME) values(%s,%s,%s,now())"

        self.mysql_helper.batch_operate(sql,rdd)



    def demo(self):
        df = self.load_from_mysql('t_CMMS_CREDIT_LIFE').select('ACCTNBR','MONTH_NBR')
        df2= df

        df.join(df2,'ACCTNBR')

        print(df.take(10))



if __name__ == '__main__':
    c = Credit()
    # c.life_cycle(2016,7)
    # c.fpgrowth()
    # c.credit_loyalty(2016,0)
    # c.tran_stat(2016,0)
    #c.tran_stat(2016,0)
    #c.cycle_credit(2016,0)


    c.credit_value()
