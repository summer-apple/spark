__author__ = 'jjzhu'
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import Row
from pyspark.mllib.fpm import FPGrowth
from pyspark.sql import DataFrameWriter

from decimal import Decimal
from enum import Enum, unique

import os
import datetime
import sched
import time
import logging
import logging.config

try:
    # 引用mysql连接模块
    from mysqlconn import MysqlConnection
    # from PySparkUsage.util.logger import Logger
except ImportError:  # 如果未能成功导入
    import sys
    # 将当前的项目路径添加到系统路径变量中
    # 这里是/home/hadoop/jjzhu/workspace
    # 如果改变了的话，也要相应的改变这里的路径
    sys.path.append(os.path.abspath('../'))
    from product.mysqlconn import MysqlConnection
    # from PySparkUsage.util.logger import Logger


@unique
class BusinessType(Enum):
    CURR_DEP = 1  # 活期存款 current deposit
    FIX_TIME_DEP = 2  # 定期存款 fixed deposit
    MONEY_MMG = 3  # 理财 money management


class DataAnalysis:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("Data Analysis")
                     .set("spark.executor.cores", '1')
                     # 指定mysql 驱动jar包
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar')
                     .set("spark.shuffle.blockTransferService", "nio")
                     )
        self.sc = SparkContext(conf=self.conf)
        self.sql_context = SQLContext(self.sc)
        # 批量插入
        self.BATCH_SIZE = 10

        # 链接到数据库
        self.mysqlconn = MysqlConnection(db='core', host='10.9.29.212', passwd='')
        self.mysql_url = 'jdbc:mysql://10.9.29.212:3306/core?user=root&characterEncoding=UTF-8'
        logging.config.fileConfig('./conf/logging.conf')
        self.logger = logging.getLogger('simpleLogger')
        self.hdfs_root_url = 'hdfs://master:9000'
        self.local_root_url = '/data/jjzhu'

        # self.logger = Logger()

    def load_from_mysql(self, db, dbtable):
        """
        通过指定mysql将数据库中的表加载为DataFrame
        :param db: 数据库名
        :param dbtable: 表名
        :return: DataFrame
        """
        url = "jdbc:mysql://10.9.29.212:3306/"+db+"?user=root&characterEncoding=UTF-8"
        df = self.sql_context.read.format("jdbc").options(url=url,
                                                          dbtable=dbtable,
                                                          driver="com.mysql.jdbc.Driver").load()
        # df = sqlContext.read.jdbc(url, dbtable, properties=properties)
        return df

    def update_acc_detail(self):
        """
        更新客户明细表中 有些只有15位账号而没有22位账号的  和 有些只有22位账号而没有15位账号的
        :return:
        """
        no15_update_query = u'update t_CMMS_ACCOUNT_DETAIL set ACC_NO15=%s where ACC_NO22=%s'
        no22_update_query = u'update t_CMMS_ACCOUNT_DETAIL set ACC_NO22=%s where ACC_NO15=%s'
        # 加载明细表
        acc_detail_df = self.load_from_mysql(u'core', u't_CMMS_ACCOUNT_DETAIL')
        acc_detail_df.cache()  # 缓存一下
        # 加载客户列表
        acc_list_df = self.load_from_mysql(u'core', u't_CMMS_ACCOUNT_LIST')
        # 只要客户列表中 15->22 账号的映射关系
        acc_list_filter_df = acc_list_df.select(acc_list_df.ACC_NO22.alias('l_no22'),
                                                acc_list_df.ACC_NO15.alias('l_no15'))
        acc_list_filter_df.cache()  # 缓存一下
        # 筛选出明细表中15账号位空的记录
        no15_empty_df = acc_detail_df.filter(acc_detail_df.ACC_NO15 == '').select('ACC_NO22')
        # 左外部连接两个表
        union_df = no15_empty_df.join(acc_list_filter_df,
                                      no15_empty_df.ACC_NO22 == acc_list_filter_df.l_no22,
                                      'left_outer')
        result = []

        for row in union_df.collect():
            row_dic = row.asDict()

            if result.__len__() >= self.BATCH_SIZE:  # 批量插入
                self.mysqlconn.execute_many(no15_update_query, result)
                result.clear()  # 清空列表
            # 如果15的不为空
            if row_dic['l_no15'] is not None:
                # print(row_dic)
                # 添加到待更新列表中
                result.append((row_dic['l_no15'], row_dic['ACC_NO22']))
        if result.__len__() != 0:
            self.mysqlconn.execute_many(no15_update_query, result)
            result.clear()

        '''
        以下是更新22位账号为空的记录，操作和更新15位的一毛一样
        '''
        no22_empty_df = acc_detail_df.filter(acc_detail_df.ACC_NO22 == '').select('ACC_NO15')
        union_df = no22_empty_df.join(acc_list_filter_df,
                                      no22_empty_df.ACC_NO15 == acc_list_filter_df.l_no15,
                                      'left_outer')
        for row in union_df.take(10):
            row_dic = row.asDict()
            if result.__len__() >= self.BATCH_SIZE:
                self.mysqlconn.execute_many(no22_update_query, result)
                result.clear()
            if row_dic['l_no22'] is not None:
                print(row_dic)
                result.append((row_dic['l_no22'], row_dic['ACC_NO15']))
        if result.__len__() != 0:
            self.mysqlconn.execute_many(no22_update_query, result)
            result.clear()
        # 清缓存
        self.sql_context.clearCache()

    def update_acc_list(self):
        """
        更新t_CMMS_ACCOUNT_LIST中相关字段
        :return:
        """
        list_update_bal_query = 'update t_CMMS_ACCOUNT_LIST set BAL_AMT=%s, STATUS=%s where ACC_NO15=%s'
        list_update_stat_query = 'update t_CMMS_ACCOUNT_LIST set CLOSE_DAT=%s, CLOSE_BRNO=%s where ACC_NO22=%s'
        curr_bal_df = self.load_from_mysql('core', 'BDFMHQAB_D')
        close_acc_df = self.load_from_mysql('core', 'BDFMHQBQ_D')
        acc_list = self.load_from_mysql('core', 't_CMMS_ACCOUNT_LIST').select('ACC_NO15', 'ACC_NO22').distinct()
        curr_bal_df = curr_bal_df.select('AB01AC15', 'AB16BAL', 'AB33DATE', 'AB08ACST').sort(curr_bal_df.AB33DATE.desc())\
            .groupBy('AB01AC15').agg({'AB16BAL': 'first', 'AB33DATE': 'first', 'AB08ACST': 'first'})
        bal_join_rdd = acc_list.join(curr_bal_df, curr_bal_df.AB01AC15 == acc_list.ACC_NO15, 'left_outer')
        insert_result = []
        for row in bal_join_rdd.collect():
            if len(insert_result) >= self.BATCH_SIZE:
                self.mysqlconn.execute_many(list_update_bal_query, insert_result)
                insert_result.clear()
                print('execute')
            row_dic = row.asDict()
            print(row_dic)
            if row_dic['AB01AC15'] is not None:
                insert_result.append((row_dic['first(AB16BAL)'], row_dic['first(AB08ACST)'], row_dic['AB01AC15']))
        if len(insert_result) > 0:
            self.mysqlconn.execute_many(list_update_bal_query, insert_result)
            insert_result.clear()
        close_join_rdd = acc_list.join(close_acc_df, acc_list.ACC_NO22 == close_acc_df.BQ01AC22)
        for row in close_join_rdd.collect():
            if len(insert_result) >= self.BATCH_SIZE:
                self.mysqlconn.execute_many(list_update_stat_query, insert_result)
                insert_result.clear()
            row_dic = row.asDict()
            if row_dic['BQ01AC22'] is not None:
                insert_result.append((row_dic['BQ06DATE'], row_dic['BQ07BRNO'], row_dic['ACC_NO22']))
        if len(insert_result):
            self.mysqlconn.execute_many(list_update_stat_query, insert_result)
            insert_result.clear()
        pass

    @staticmethod
    def get_time_slot(start, end, format_='%Y-%m-%d'):
        """
        根据start,end起止时间，返回该时间内每个月的月初和月末二元组列表
        :param start: 起始时间（str）
        :param end: 结束时间(str)
        :param format_: 时间格式
        :return:时间段内每个月月初与月末二元组列表
                e.g.:
                >>start='2016-01-01'
                >>end='2016-03-01'
                >>get_time_slot(start, end)
                [('2016-01-01', '2016-01-31'), ('2016-02-01', '2016-02-30'), ('2016-03-01', '2016-03-31')]
        """
        import calendar as cal

        time_slot = []
        try:
            s = datetime.datetime.strptime(start, format_)
            e = datetime.datetime.strptime(end, format_)
        except ValueError as e:
            print(e)
            exit(-1)
        while s <= e:
            day_range = cal.monthrange(s.year, s.month)
            last_day = datetime.datetime(s.year, s.month, day_range[1], 0, 0, 0)
            time_slot.append((
                datetime.datetime(s.year, s.month, 1).strftime('%Y-%m-%d'),
                last_day.strftime('%Y-%m-%d')
            ))
            s = last_day + datetime.timedelta(1)  # 下个月
        return time_slot

    def calc_mm(self):
        df = self.load_from_mysql('core', 'FMS_BATCH_CAPITAL_LOG_D')
        start_time = '20160201'  # TODO 改为最早记录的时间
        # str(df.select('ORIGIN_TRANS_DATE').sort(df.ORIGIN_TRANS_DATE.asc()).take(1)[0].ORIGIN_TRANS_DATE)
        start_time = datetime.datetime.strptime(start_time, '%Y%m%d').__format__('%Y-%m-%d')
        end_time = datetime.datetime.now().__format__('%Y-%m-%d')
        time_slots = self.get_time_slot(start_time[:start_time.rindex('-')], end_time[:end_time.rindex('-')], '%Y-%m')
        print(time_slots)
        del df
        for time_slot in time_slots:
            # print('call procedure Init_Balance (%s, %s)' % (str(time_slot), str(3)))
            print('call procedure Init_Balance (%s, %s)' % (str(time_slot), str(BusinessType.MONEY_MMG.value)))
            self.init_balance(str(time_slot[0]),  BusinessType.MONEY_MMG)
            # self.init_balance(time_slot[0], BusinessType.MONEY_MMG)
            # self.mysqlconn.call_proc(proc_name='Init_Balance', args=('\''+str(time_slot[0])+'\''))
            # self._calc_bal_by_type(time_slot[0], BusinessType.MONEY_MMG)

    def calc_CA(self):
        end_time = datetime.datetime.now().__format__('%Y-%m-%d')
        data_frame = self.load_from_mysql('core', 'BDFMHQAB_D')
        start_time = str(data_frame.select('AB33DATE').sort(data_frame.AB33DATE.asc()).take(1)[0].AB33DATE)
        # 找出处理的最晚时间
        # end_time = str(data_frame.select('AB33DATE').sort(data_frame.AB33DATE.desc()).take(1)[0].AB33DATE)
        # 得到开始时间 到 结束时间 之间的月份的时间段
        time_slots = self.get_time_slot(start_time[:start_time.rindex('-')], end_time[:end_time.rindex('-')], '%Y-%m')

        print(time_slots)
        # self.mysqlconn.execute_single('call Init_Balance(%s, %s)', ('2016-07-01',  str(1)))
        # self._calc_bal_by_type('2016-07-01', 1)

        for time_slot in time_slots:
            self.logger.info('call method: init_balance (%s, %s)' % (str(time_slot[0]), str(BusinessType.CURR_DEP.value)))
            self.init_balance(str(time_slot[0]), BusinessType.CURR_DEP)
            # self._calc_bal_by_type(time_slot[0], BusinessType.CURR_DEP)

    def calc_FA(self):

        data_frame = self.load_from_mysql('core', 'BFFMDQAB_D')
        start_time = '2016-02-01'  # TODO 改成注释中的
        # str(data_frame.select('AB30DATE').sort(data_frame.AB30DATE.asc()).take(1)[0].AB30DATE)
        end_time = datetime.datetime.now().__format__('%Y-%m-%d')
        time_slots = self.get_time_slot(start_time[:start_time.rindex('-')], end_time[:end_time.rindex('-')], '%Y-%m')
        for time_slot in time_slots:

            # print('call procedure Init_Balance (%s, %s)' % (str(time_slot[0]), str(BusinessType.FIX_TIME_DEP.value)))

            # self.mysqlconn.execute_single('call Init_Balance(%s, %s)', (str(time_slot[0]),  str(2)))

            self.init_balance(time_slot[0], BusinessType.FIX_TIME_DEP)
            # self._calc_bal_by_type(time_slot[0], BusinessType.FIX_TIME_DEP)

    def update_asset_info(self):
        """
        更新资产表中CUST_NO为空的记录
        :return:
        """
        # TODO 将t_CMMS_ASSLIB_ASSET_c改成t_CMMS_ASSLIB_ASSET
        asset_update_query = 'update t_CMMS_ASSLIB_ASSET_c set ACC_NAM=%s, CUST_ID=%s where CUST_NO=%s and ACC_NAM is NULL'
        temp_df = self.load_from_mysql('core', 't_CMMS_ASSLIB_ASSET_c')
        cust_no_df = temp_df.filter('ACC_NAM is null').select(temp_df.CUST_NO).distinct()
        customer_info_df = self.load_from_mysql('core', ' t_CMMS_INFO_CUSTOMER')
        customer_info_df = customer_info_df.select(customer_info_df.CUST_ID, customer_info_df.CUST_NAM,
                                                   customer_info_df.CUST_NO.alias('cust_no'))
        join_df = cust_no_df.join(customer_info_df, cust_no_df.CUST_NO == customer_info_df.cust_no, 'left_outer')
        update_result = []
        for row in join_df.collect():

            row_dict = row.asDict()
            if row_dict['cust_no'] is not None and row_dict['CUST_NAM'] is not None:
                print(row_dict)

                if update_result.__len__() >= self.BATCH_SIZE:
                    self.mysqlconn.execute_many(asset_update_query, update_result)
                    update_result.clear()

                update_result.append((row_dict['CUST_NAM'] if row_dict['CUST_NAM'] is not None else '',
                                      row_dict['CUST_ID'] if row_dict['CUST_ID'] is not None else '',
                                      row_dict['CUST_NO']))
        if update_result.__len__() > 0:
            self.mysqlconn.execute_many(asset_update_query, update_result)
            update_result.clear()

    def aum2(self):
        """
        在系统初始化的时候调用这个方法（第一次运行的时候）
        :return:
        """
        # self.mysqlconn.execute_single('call truncate_d()')
        # self.mysqlconn.execute_single('call filter()')

        # 慎用！
        # self.logger.info('calling Drop_all()')
        # self.mysqlconn.execute_single('call Drop_all()')

        # self.logger.info('开始计算活期存款AUM')

        # 计算定期
        self.calc_FA()
        # 计算理财的
        self.calc_mm()
        # 计算活期

        self.calc_CA()
        # 更新asset表中缺失的字段
        self.update_asset_info()
        # 找出处理的最早时间
        # start_time = str(data_frame.select('AB33DATE').sort(data_frame.AB33DATE.asc()).take(1)[0].AB33DATE)

    def stat_act_time(self):
        """
        这里是统计每张卡每个月每种业务（定、活、理）办理的次数
        :return:
        """
        target_table = 't_CMMS_TEMP_ACTTIME'
        save_mode = 'append'
        acc_detail_source = self.sc.textFile('%s/jjzhu/test/input/t_CMMS_ACCOUNT_DETAIL.txt' % self.hdfs_root_url)
        # 这里是更新t_CMMS_TEMP_ACTTIME表的
        acc_list = self.load_from_mysql('core', 't_CMMS_ACCOUNT_LIST')\
            .map(lambda row: (row.asDict()['ACC_NO15'], (row.asDict()['CUST_NO'], row.asDict()['ACC_NAM']))).distinct()
        acc_list.cache()  # 添加缓存
        split_rdd = acc_detail_source.map(lambda line: line.split(','))
        split_rdd.cache()  # 添加缓存
        start_time = '2016-02-01'  # TODO 之后得改成最早的时间
        end_time = '2016-08-01'  # TODO 最晚的时间
        time_slots = self.get_time_slot(start_time, end_time)
        for slot in time_slots:
            self.logger.info('statistic action time of %s' % slot[0][0: slot[0].rindex('-')])
            # 以 客户号+业务类型为key，统计客户不同类型的交易次数
            act_time = split_rdd.filter(lambda columns: columns[1] <= slot[1])\
                .filter(lambda columns: columns[1] >= slot[0])\
                .map(lambda columns: (columns[3]+'_'+columns[6], 1))\
                .reduceByKey(lambda a, b: a+b)

            mapped_act_time = act_time.map(lambda fields:
                                           (fields[0].split('_')[0],
                                            (fields[0].split('_')[0], fields[0].split('_')[1],
                                             fields[1], slot[0][0: slot[0].rindex('-')])))
            # join操作，连接客户号对应的具体信息
            result = mapped_act_time.join(acc_list).map(lambda fields: fields[1][0] + fields[1][1])
           
            #  ACCT_NO15， BUST_TYPE, ACT_TIME, CURR_MONTH, CUST_NO, ACCT_NAM
            # '101006463122653', '1', 25, '2016-02-01', '81024082971', '曹镇颜'

            result_writer = DataFrameWriter(
                self.sql_context.createDataFrame(result, ['ACCT_NO15', 'BUST_TYPE', 'ACT_TIME',
                                                          'CURR_MONTH', 'CUST_NO', 'ACCT_NAM']))
            self.logger.info('save the statistic result into mysql\n'
                             'url: %s\n'
                             'target table: %s\n'
                             'mode: %s' % (self.mysql_url, target_table, save_mode))
            result_writer.jdbc(self.mysql_url, table=target_table, mode=save_mode)

    # TODO start_time 默认值，end_time 默认值
    def loyalty(self, start_time='2016-04-01', end_time='2016-07-31'):
        """
        统计忠诚度
        同数据库中t_CMMS_TEMP_ACTTIME表中加载数据，然后按照时间段统计每个账户总的交易次数
            总次数在[0-1]忠诚度等级为1
                 在[2-4]忠诚度等级为2
                 大于5等级为3
        :return:
        """
        curr_time = datetime.datetime.now()
        temp_act_time_tf = self.load_from_mysql('core', 't_CMMS_TEMP_ACTTIME')
        cust_info_df = self.load_from_mysql('core', 't_CMMS_INFO_CUSTOMER').distinct()

        cust_info_df = cust_info_df.select(cust_info_df.CUST_NO.alias('c_cust_no'), cust_info_df.CUST_ID,
                                           cust_info_df.CUST_NAM)
        loyalty_insert_query = 'insert into t_CMMS_ANALYSE_LOYALTY(CUST_ID, CUST_NO, CUST_NM, LOYALTY, MONTH, ' \
                               'UPDATE_TIME) value (%s, %s, %s, %s, %s, %s)'
        # 根据CUST_NO分组并对ACT_TIME求和
        sum_result = temp_act_time_tf.filter(temp_act_time_tf.CUST_NO != '')\
            .filter(temp_act_time_tf.CURR_MONTH >= start_time[:start_time.rindex('-')])\
            .filter(temp_act_time_tf.CURR_MONTH <= end_time[:end_time.rindex('-')])\
            .groupBy('CUST_NO')\
            .sum('ACT_TIME')
        # 连接客户信息（有些字段为空的）
        join_result_df = sum_result.join(cust_info_df, cust_info_df.c_cust_no == sum_result.CUST_NO, 'left_outer')
        insert_record = []
        # join_result_df = join_result_df.filter(join_result_df.CUST_NO == '81022473012')
        for row in join_result_df.collect():  # TODO  collect()
            row_dic = row.asDict()
            if insert_record.__len__() >= self.BATCH_SIZE:
                #  'update core.LOYALTY set SUMTIME=%s, LOYGRADE=%s, CURRDATE=%s where CSTMNO=%s'
                self.mysqlconn.execute_many(loyalty_insert_query, insert_record)
                insert_record.clear()
            # 次数为 [0, 1]
            if row_dic['sum(ACT_TIME)'] < 2:
                grade = 1
            # 次数在 [2, 4]
            elif row_dic['sum(ACT_TIME)'] < 5:
                grade = 2
            # 次数大于4的
            else:
                grade = 3
            insert_record.append((str(row_dic['CUST_ID']), str(row_dic['CUST_NO']), str(row_dic['CUST_NAM']),
                                  str(grade), str(datetime.datetime.strptime(end_time, '%Y-%m-%d').strftime('%Y%m%d')),
                                  str(curr_time)))
        # 如果列表中还有未处理的数据
        if insert_record.__len__() > 0:
            self.mysqlconn.execute_many(loyalty_insert_query, insert_record)
            insert_record.clear()
        print('end')

    def aum_time_task(self):
        scheduler = sched.scheduler(timefunc=time.time, delayfunc=time.sleep)

        def ca_job():
            scheduler.enter(24*60*60, 0, ca_job)
            print(datetime.datetime.now())
            _time = str(datetime.datetime.now().strftime('%Y-%m-%d'))
            print('call init_balance(%s, %s)' % (_time, BusinessType.CURR_DEP))

            # self.mysqlconn.execute_single('call Calc_balance(%s, %s)', (_time, str(1)))
            self.init_balance(_time, 1)

        def fa_job():
            scheduler.enter(24*60*60, 0, fa_job)
            print(datetime.datetime.now())
            _time = str(datetime.datetime.now().strftime('%Y-%m-%d'))
            print('call init_balance(%s, %s)' % (_time, BusinessType.FIX_TIME_DEP))
            # self.mysqlconn.execute_single('call Calc_balance(%s)', (_time, str(2)))
            self.init_balance(_time, 2)

        def start():
            now = datetime.datetime.now()
            late = datetime.datetime(now.year, now.month, now.day, 16, 29)
            if late > now:
                time_delay = (late-now).seconds
                print('time delay '+str(time_delay) + ' s')
                time.sleep((late-now).seconds)
            fa_job()
            ca_job()
            scheduler.run()
        start()

    def end(self):
        self.mysqlconn.close()

    def init_balance(self, ctime, tp):
        """
        处理流程：
            方法接收 格式为'%Y-%m-%d'的ctime和业务类型tp
            while 当前月：
                调用存储过程 Calc_balance（该存储过程更新当前日期的余额）
                将当前余额保存到hdfs中
                天数+1
            将当前月每一天的余额复制到本地文件中进行合并xxxx_xx.data
            将xxxx_xx.data合并文件移到hdfs中去
            读取该文件，进行日均余额的计算，并保存结果到hdfs中
            将结果合并至一个文件xxxx_xx-r.data
            加载结果合并文件xxxx_xx-r.data，导入mysql数据库

        :param ctime:格式为'%Y-%m-%d'的ctime
        :param tp: 业务类型
        :return:
        """

        try:

            cdate = datetime.datetime.strptime(ctime, '%Y-%m-%d')
            end_date = cdate
            # 合并后的文件名为 年份_月份.data
            merged_file_name = '%s.data' % cdate.strftime('%Y_%m')

            result_file_name = '%s-r.data' % cdate.strftime('%Y_%m')
            # hdfs输入路径
            hdfs_root_path = '%s/jjzhu/test/_%s/%s/' % (self.hdfs_root_url, str(tp.value), cdate.strftime('%Y_%m'))
            # 本地的临时目录路径
            local_temp_path = '%s/temp/_%s/' % (self.local_root_url, str(tp.value))

            def exist_or_create(path):
                if not os.path.exists(path):
                    self.logger.warning('local path: %s is not exist' % path)
                    self.logger.info('creating dir: %s' % path)
                    os.makedirs(path)
                self.logger.info('local path: %s is already existed' % path)

            exist_or_create(local_temp_path)

            # 本地合并后的文件的输出路径
            local_output_path = '%s/output/_%s/' % (self.local_root_url, str(tp.value))
            exist_or_create(local_output_path)

            local_result_path = '%s/result/_%s/' % (self.local_root_url, str(tp.value))
            exist_or_create(local_result_path)
            # AUM计算结果的hdfs输出路径
            hdfs_save_path = '%s/jjzhu/test/result/_%s/%s' % (self.hdfs_root_url,
                                                              str(tp.value), cdate.strftime('%Y_%m'))
            # 计算AUM原文件的hdfs输入路径
            hdfs_input_path = '%s/jjzhu/test/input/_%s/' % (self.hdfs_root_url, str(tp.value))
            if os.system('hadoop fs -mkdir %s' % hdfs_input_path) == 0:
                self.logger.warning('%s is not exist, created it successful' % hdfs_input_path)
            target_save_table = 't_CMMS_ASSLIB_ASSET_c'  # TODO
            save_mode = 'append'
            local_output_file_path = local_output_path+merged_file_name
            local_dir_for_merge = local_temp_path + cdate.strftime('%Y_%m')

            # Row()转换成tuple
            def change_row(row):
                row_dict = row.asDict()
                return (row_dict['CUST_NO'], row_dict['ACCT_NO15'], str(row_dict['CURR_BAL']),
                        str(row_dict['TIME']), row_dict['CUR'],
                        row_dict['CURR_DATE'].strftime('%Y-%m-%d'),
                        str(row_dict['BUSI_TYPE']))

            def calc_avg(el):
                r = el[1]
                return (r[0], r[1], float(r[2])/int(r[3]), r[4],
                        datetime.datetime.strptime(str(r[5]), '%Y-%m-%d').strftime('%Y-%m'), r[6])

            # 将hdfs上每个月的数据移到本地
            def copy_from_hdfs_to_local(hdfs_dir, local_dir):
                import sys
                if not os.path.exists(local_dir):
                    self.logger.warning('%s is not existed, create it first.' % local_dir)
                    os.system('mkdir '+local_dir)
                    # exit(-1)
                shell_command = 'hadoop fs -copyToLocal %s %s' % (hdfs_dir, local_dir)
                self.logger.info('execute hdfs shell command: %s' % shell_command)
                os.system(shell_command)

            # 合并文件
            def merge_file(input_dir, output_path):
                import platform
                if output_path.__contains__('/data'):
                    output_file = open(output_path, 'w+')
                    if platform.system() == 'Windows':
                        deli = '\\'
                    elif platform.system() == 'Linux':
                        deli = '/'
                    else:
                        self.logger.error('unknown platform: %s' % platform.system())
                    for i in os.listdir(input_dir):
                        curr_dir = input_dir+deli+i
                        for j in os.listdir(curr_dir):
                            if j.startswith('part'):  # 只合并以part开头的输出文件
                                with open(curr_dir+deli+j) as f:
                                    # print(curr_dir+deli+j)
                                    for line in f.readlines():
                                        # print(line[1:len(line)-2])
                                        output_file.write(line[1:len(line)-2]+'\n')

                    output_file.close()
                else:
                    self.logger.error('please make sure your input path is under /data')
                    exit(-1)

            while end_date.month == cdate.month:
                # print(end_date)
                hdfs_path = hdfs_root_path + end_date.strftime('%Y_%m_%d')
                # 调用mysql存储过程，更新当前余额
                self.logger.info('call Calc_balance(%s, %s)' %
                                 (end_date.strftime('%Y-%m-%d'), str(tp.value)))
                self.mysqlconn.execute_single('call Calc_balance(%s, %s)',
                                              (end_date.strftime('%Y-%m-%d'), str(tp.value)))
                # 获取当前余额
                curr_bal_df = self.load_from_mysql('core', 't_CMMS_TEMP_DAYBAL_T').select(
                    'CUST_NO', 'ACCT_NO15', 'CURR_BAL', 'TIME', 'CUR', 'CURR_DATE', 'BUSI_TYPE'
                )

                curr_bal_rdd = curr_bal_df.filter(curr_bal_df.BUSI_TYPE == tp.value).map(change_row)
                print(curr_bal_rdd.take(1))
                if curr_bal_rdd.count() == 0:
                    self.logger.warning('rdd is empty')
                    continue
                if os.system('hadoop fs -test -e '+hdfs_path) == 0:
                    self.logger.warning('%s is already existed, deleting' % hdfs_path)
                    if os.system('hadoop fs -rm -r '+hdfs_path) == 0:
                        self.logger.info('delete %s successful' % hdfs_path)
                # 保存当前余额信息到hdfs上以便合并
                self.logger.info('save rdd context to %s' % hdfs_path)
                curr_bal_rdd.saveAsTextFile(hdfs_path)
                end_date += datetime.timedelta(1)
            # 将当前月移到本地进行合并
            self.logger.info('copy file from %s to %s' % (hdfs_root_path, local_temp_path))
            copy_from_hdfs_to_local(hdfs_root_path, local_temp_path)
            self.logger.info('merge file content\n\tinput dir: %s\n\toutput_file: %s' %
                             (local_dir_for_merge, local_output_file_path))
            merge_file(local_dir_for_merge, local_output_file_path)
            # 合并后的文件移回hdfs作为AUM的输入
            if os.system('hadoop fs -test -e '+hdfs_input_path) == 1:
                self.logger.warning('hdfs dir: %s is not exist' % hdfs_input_path)
                self.logger.info('\texecute hdfs command: hadoop fs -mkdir %s' % hdfs_input_path)
                os.system('hadoop fs -mkdir '+hdfs_input_path)
            if os.system('hadoop fs -test -e ' + hdfs_input_path+merged_file_name) == 0:
                self.logger.info('hdfs file: %s is already exist' % hdfs_input_path+merged_file_name)
                self.logger.info('\texcute hdfs command: hadoop fs -rm ' + hdfs_input_path+merged_file_name)
                os.system('hadoop fs -rm ' + hdfs_input_path+merged_file_name)
            os.system('hadoop fs -put ' + local_output_file_path + ' ' + hdfs_input_path)

            # 计算AUM
            self.logger.info('start calculate AUM of %s' % cdate.strftime('%Y-%m'))
            all_data = self.sc.textFile(hdfs_input_path+merged_file_name)

            day_bal = all_data.map(lambda line: line.strip().split(','))\
                .map(lambda fields: (fields[0].strip()[1:len(fields[0].strip())-1],  # CUST_NO
                                     fields[1].strip()[1:len(fields[1].strip())-1],  # ACCT_NO15
                                     float(fields[2].strip()[1:len(fields[2].strip())-1]),  # CURR_BAL
                                     1,  # TIME
                                     fields[4].strip()[1:len(fields[4].strip())-1],  # CUR
                                     fields[5].strip()[1:len(fields[5].strip())-1],  # CURR_DATE
                                     fields[6].strip()[1:len(fields[6].strip())-1],  # BUSI_TYPE
                                     ))
            # 用ACCT_NO15位主键，并累加余额和次数，然后在计算日均余额
            add_bal = day_bal.map(lambda fields: (fields[1], fields))\
                .reduceByKey(lambda a, b: (a[0], a[1], float(float(a[2])+float(b[2])), int(a[3])+int(b[3]), a[4], max(a[5], b[5]), a[6]))\
                .map(calc_avg)
            # 判断保存目录路径是否存在，若存在，则将其删除，否则savaAsTexstFile操作会报错
            if os.system('hadoop fs -test -e ' + hdfs_save_path) == 0:
                os.system('hadoop fs -rm -r ' + hdfs_save_path)
            # add_bal.cache()
            add_bal.saveAsTextFile(hdfs_save_path)

            os.system('hadoop fs -getmerge %s %s%s' % (hdfs_save_path, local_result_path, result_file_name))
            os.system('hadoop fs -put %s%s %s' % (local_result_path, result_file_name,
                                                  hdfs_input_path))

            result_rdd = self.sc.textFile(hdfs_input_path+result_file_name)
            mapped_rdd = result_rdd.map(lambda line: line[1: len(line)-1])\
                .map(lambda line: line.split(','))\
                .map(lambda fields: (fields[0].strip()[1:len(fields[0].strip())-1],
                                     fields[1].strip()[1:len(fields[1].strip())-1],  # ACCT_NO15
                                     float(fields[2].strip()[0:len(fields[2].strip())]),  # AUM
                                     fields[3].strip()[1:len(fields[3].strip())-1],  # CUR
                                     fields[4].strip()[1:len(fields[4].strip())-1],  # STAT_DAT
                                     fields[5].strip()[1:len(fields[5].strip())-1],  # ASS_TYPE
                                     ))
            # 重新创建对应的DataFrame并创建对应的writer
            writer = DataFrameWriter(self.sql_context.createDataFrame(mapped_rdd, ['CUST_NO', 'ACC_NO15', 'AUM', 'CUR',
                                                                                   'STAT_DAT', 'ASS_TYPE']))
            # self.mysql_url = 'jdbc:mysql://10.9.29.212:3306/core?user=root&characterEncoding=UTF-8'
            # 将DF中的内容以指定的方式保存到指定的表中
            writer.jdbc(self.mysql_url, table=target_save_table, mode=save_mode)
        except ValueError as e:
            self.logger.error(e)
            exit(-1)

    def test(self):
        import os
        os.system('cp -r ../dict /home/hadoop')
        os.system('scp -r /home/hadoop/dict slave1:/home/hadoop')
        os.system('scp -r /home/hadoop/dict slave2:/home/hadoop')
        contents = open('../dict/stop_word.txt', 'rb').read().decode('utf-8')
        stop_words = set()
        for line in contents.splitlines():
            stop_words.add(line)
        # 广播变量
        sw = self.sc.broadcast(stop_words)

        def most_freq(desc):
            import jieba

            jieba.load_userdict('/home/hadoop/dict/my.dic')

            freq_dict = {}

            for l in desc:
                seg_list = jieba.cut(l, cut_all=False)
                for seg in seg_list:
                    if len(seg.strip()) == 0 or seg in sw.value or seg.isdigit():
                        continue
                    if seg not in freq_dict:
                        freq_dict[seg] = 1
                    else:
                        freq_dict[seg] += 1
            filter_result = filter(lambda elem: elem[1] > 1, freq_dict.items())
            result_dict = dict([item for item in filter_result])
            return sorted(result_dict.items(), key=lambda item: item[1], reverse=True)

        tran_data = self.load_from_mysql('core', 't_CMMS_CREDIT_TRAN')\
            .filter('BILL_AMTFLAG = \'+\'').select('CARD_NBR', 'DES_LINE1')\
            .map(lambda r: (r.asDict()['CARD_NBR'].strip(), [r.asDict()['DES_LINE1'].strip()]))\
            .reduceByKey(lambda a, b: a + b).map(lambda elem: (elem[0], most_freq(elem[1])))\
            .filter(lambda elem: len(elem[1]) > 0)

        for row in tran_data.take(10):
            print(row)


if __name__ == '__main__':
    ana = DataAnalysis()

    ana.aum2()
    ana.stat_act_time()
    ana.loyalty()

    ana.end()

