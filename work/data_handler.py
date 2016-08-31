import pydevd
from pyspark import SparkContext, SparkConf, SQLContext
import datetime
from pyspark.sql.types import Row
try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os

    sys.path.append(os.path.abspath('../'))
    from work.mysql_helper import MySQLHelper

#pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)


class DataHandler:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("Summer")
                     .set("spark.cores.max", "2")
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)

        self.mysql_helper = MySQLHelper('core', host='10.9.29.212')

    def load_from_mysql(self, table, database='core'):
        url = "jdbc:mysql://10.9.29.212:3306/%s?user=root&characterEncoding=UTF-8" % database
        df = self.sqlctx.read.format("jdbc").options(url=url, dbtable=table, driver="com.mysql.jdbc.Driver").load()
        return df

    def prepare_life_cycle(self, year, season):
        '''
        prepare data

        saum1 (last season sum aum)
        saum2 (current season sum aum)
        aum_now
        account_age (months)
        last_tr_date (days)

        :param year:
        :param season: 1,2,3,4
        :return:
        '''

        # 计算月份
        print('----------------------生命周期-Start----------------------')

        print('开始准备生命周期数据...')
        print('开始计算月份')

        if season == 1:
            # date1 当前季度月份
            date1 = [str(year) + '-01', str(year) + '-02', str(year) + '-03']

            # date2 上一季月份
            date2 = [str(year - 1) + '-10', str(year - 1) + '-11', str(year - 1) + '-12']

        elif season == 4:
            date1 = [str(year) + '-10', str(year) + '-11', str(year) + '-12']
            date2 = [str(year) + '-07', str(year) + '-08', str(year) + '-9']

        else:
            date1 = [str(year) + '-0' + str(3 * season - 2), str(year) + '-0' + str(3 * season - 1),
                     str(year) + '-0' + str(3 * season)]
            date2 = [str(year) + '-0' + str(3 * season - 5), str(year) + '-0' + str(3 * season - 4),
                     str(year) + '-0' + str(3 * season - 3)]

        print('当前季度月份 new：', date1)
        print('上一季度月份 old：', date2)

        # 加载AUM表
        aum = self.load_from_mysql('t_CMMS_ASSLIB_ASSET_c').cache()

        # 拼接每季度三个月断数据
        season_new = aum.filter(aum.STAT_DAT == date1[0]).unionAll(aum.filter(aum.STAT_DAT == date1[1])).unionAll(
            aum.filter(aum.STAT_DAT == date1[2]))
        season_old = aum.filter(aum.STAT_DAT == date2[0]).unionAll(aum.filter(aum.STAT_DAT == date2[1])).unionAll(
            aum.filter(aum.STAT_DAT == date2[2]))

        # 计算每季度AUM
        aum_season_old = season_old.select('CUST_NO', season_old.AUM.alias('AUM1')).groupBy('CUST_NO').sum('AUM1')
        aum_season_new = season_new.select('CUST_NO', season_new.AUM.alias('AUM2')).groupBy('CUST_NO').sum('AUM2')

        # 两个季度进行外联接
        '''
        +-----------+---------+---------+
        |    CUST_NO|sum(AUM2)|sum(AUM1)|
        +-----------+---------+---------+
        |81005329523|     null|294844.59|
        |81011793167|     null|   365.20|
        |81015319088|     null|  9640.96|
        +-----------+---------+---------+
        '''
        union_season = aum_season_old.join(aum_season_new, 'CUST_NO', 'outer')

        # 筛选当前AUM
        temp_result = aum.select('CUST_NO', 'AUM', 'STAT_DAT').groupBy('CUST_NO', 'STAT_DAT').sum('AUM').sort(
            'CUST_NO').sort(aum.STAT_DAT.desc())
        temp_result.select('CUST_NO', temp_result['sum(AUM)'].alias('AUM'), 'STAT_DAT').registerTempTable('group_in')

        aum_now_sql = "select CUST_NO,first(AUM) as AUM_NOW from group_in group by CUST_NO"

        aum_now = self.sqlctx.sql(aum_now_sql)
        # 清除缓存表
        self.sqlctx.dropTempTable('group_in')

        # 联合
        union_season_aumnow = union_season.join(aum_now, 'CUST_NO', 'outer')

        # 计算用户开户至今时间(months)
        # 载入账户表
        account = self.load_from_mysql('t_CMMS_ACCOUNT_LIST').cache()
        account.select('CUST_NO', 'OPEN_DAT').registerTempTable('account')
        account_age_aql = "select  CUST_NO, first(ACCOUNT_AGE) as ACCOUNT_AGE  from " \
                          "(select CUST_NO, round(datediff(now(), OPEN_DAT) / 30) as ACCOUNT_AGE " \
                          "from account order by CUST_NO, ACCOUNT_AGE desc ) as t group by CUST_NO"

        account_age = self.sqlctx.sql(account_age_aql)

        # calculate last tran date
        account_1 = account.select('CUST_NO', 'ACC_NO15')
        detail = self.load_from_mysql('t_CMMS_ACCOUNT_DETAIL').select('ACC_NO15', 'TRAN_DAT')
        a_d = account_1.join(detail, 'ACC_NO15', 'outer')
        a_d.filter(a_d.CUST_NO != '').registerTempTable('adtable')

        last_tr_date_sql = "select CUST_NO,first(TRAN_DAT) as LAST_TR_DATE from (select CUST_NO,TRAN_DAT from adtable order by TRAN_DAT desc) as t group by CUST_NO"

        last_tr_date = self.sqlctx.sql(last_tr_date_sql)

        # 联合 season   aum_now    account_age     last_tr_date
        unions = union_season_aumnow.join(account_age, 'CUST_NO', 'outer').join(last_tr_date, 'CUST_NO', 'outer')

        # 清除缓存表
        self.sqlctx.dropTempTable('account')
        self.sqlctx.dropTempTable('adtable')
        self.sqlctx.clearCache()

        # 结果插入表
        print('结果插入临时表：t_CMMS_TEMP_LIFECYCLE...')
        insert_lifecycle_sql = "replace into t_CMMS_TEMP_LIFECYCLE(CUST_NO,SAUM1,SAUM2,INCREASE,ACCOUNT_AGE,AUM_NOW,LAST_TR_DATE) values(%s,%s,%s,%s,%s,%s,%s)"

        # 缓冲区
        temp = []
        for row in unions.collect():
            row_dic = row.asDict()

            if len(temp) >= 1000:  # 批量写入数据库
                self.mysql_helper.executemany(insert_lifecycle_sql, temp)
                temp.clear()

            # 加载数据到缓冲区

            try:
                # 计算增长率
                increase = (row_dic['sum(AUM2)'] - row_dic['sum(AUM1)']) / row_dic['sum(AUM1)']
            except Exception:
                increase = 0

            # 计算开户时长（月份数） 若无则视为6个月以上
            if row_dic['ACCOUNT_AGE'] is None:
                row_dic['ACCOUNT_AGE'] = 7

            # 最后交易日期
            ltd = row_dic['LAST_TR_DATE']
            if ltd is not None:
                try:
                    ltd = datetime.datetime.strptime(ltd, '%Y-%m-%d')
                except Exception:
                    ltd = ltd[:4] + '-' + ltd[4:6] + '-' + ltd[6:]
                    ltd = datetime.datetime.strptime(ltd, '%Y-%m-%d')

                days = (datetime.datetime.now() - ltd).days
            else:
                days = 366

            temp.append((row_dic['CUST_NO'], row_dic['sum(AUM1)'], row_dic['sum(AUM2)'], increase,
                         row_dic['ACCOUNT_AGE'], row_dic['AUM_NOW'], days))

        if len(temp) != 0:
            self.mysql_helper.executemany(insert_lifecycle_sql, temp)
            temp.clear()

        self.calculate_life_cycle()
        self.lifecycle_to_real_table(year, season)

    def calculate_life_cycle(self):

        '''
        calculate life cycle period
        :return:
        '''

        print('开始计算生命周期...')
        life_cycle = self.load_from_mysql('t_CMMS_TEMP_LIFECYCLE').cache()

        def clcmap(line):
            cust_no = line['CUST_NO']
            account_age = line['ACCOUNT_AGE']
            last_tr_date = line['LAST_TR_DATE']
            aum_now = line['AUM_NOW']
            increase = line['INCREASE']

            period = 0
            if aum_now is None:
                period = 9  # 未知
            elif aum_now < 1000 and last_tr_date > 365:
                period = 3  # 流失期
            else:
                if increase > 20 or account_age < 6:
                    period = 0  # 成长期
                elif increase >= -20 and increase <= 20:
                    period = 1  # 成熟期
                else:
                    period = 2  # 稳定期

            return period, cust_no


        map_result = life_cycle.map(clcmap).collect()

        # clear the life_cycle cache
        self.sqlctx.clearCache()

        temp = []
        print('结果更新到临时表：t_CMMS_TEMP_LIFECYCLE...')
        update_life_period_sql = "update t_CMMS_TEMP_LIFECYCLE set PERIOD = %s where CUST_NO = %s"
        for row in map_result:

            if len(temp) >= 1000:
                self.mysql_helper.executemany(update_life_period_sql, temp)
                temp.clear()
            temp.append(row)

        if len(temp) != 1000:
            self.mysql_helper.executemany(update_life_period_sql, temp)
            temp.clear()

    def lifecycle_to_real_table(self, year, season):
        '''
        put life_cycle tmp table to real table
        :return:
        '''

        print('开始将生命周期数据写入正式表中...')
        life_cycle = self.load_from_mysql('t_CMMS_TEMP_LIFECYCLE').select('CUST_NO', 'PERIOD')
        cust_info = self.load_from_mysql('t_CMMS_INFO_CUSTOMER').select('CUST_NO', 'CUST_ID', 'CUST_NAM')

        union = life_cycle.join(cust_info, 'CUST_NO', 'left_outer').cache()

        temp = []
        sql = "replace into t_CMMS_ANALYSE_LIFE(CUST_NO,CUST_ID,CUST_NM,LIFE_CYC,QUARTER,UPDATE_TIME) values(%s,%s,%s,%s,%s,now())"
        for row in union.collect():

            if len(temp) >= 1000:
                self.mysql_helper.executemany(sql, temp)
                temp.clear()
            quarter = str(year) + '-' + str(season)

            cust_id = row['CUST_ID'] if row['CUST_ID'] is not None else '0'
            temp.append((row['CUST_NO'], cust_id, row['CUST_NAM'], row['PERIOD'], quarter))

        if len(temp) != 1000:
            self.mysql_helper.executemany(sql, temp)
            temp.clear()

        self.sqlctx.clearCache()

    def customer_value(self, year, half_year):
        '''
        calculate customer value
        :param year: which year to calculate
        :param half_year: 0 for month 1-6 , 1 for month 7-12
        :return:
        '''

        print('---------------------------客户价值-Start--------------------------')
        cust_info = self.load_from_mysql('t_CMMS_INFO_CUSTOMER').select('CUST_NO', 'CUST_ID', 'CUST_NAM').cache()
        aum = self.load_from_mysql('t_CMMS_ASSLIB_ASSET_c').select('CUST_NO', 'STAT_DAT', 'AUM', 'ASS_TYPE').cache()

        base = half_year * 6

        aum_slot_filter = None

        for i in range(1, 7):
            i = base + i
            if i < 10:
                i = '0' + str(i)
            else:
                i = str(i)
            slot = str(year) + '-' + i

            slot_filter = aum.filter(aum.STAT_DAT == slot)
            if aum_slot_filter is None:
                aum_slot_filter = slot_filter
            else:
                aum_slot_filter = aum_slot_filter.unionAll(slot_filter)

        # CUST_NO sum(AUM)
        huoqi_aum = aum_slot_filter.select('CUST_NO', 'ASS_TYPE', aum_slot_filter['AUM'].alias('AUM_HQ')).filter(
            aum_slot_filter.ASS_TYPE == '1').groupBy('CUST_NO').sum('AUM_HQ')
        dingqi_aum = aum_slot_filter.select('CUST_NO', 'ASS_TYPE', (aum_slot_filter.AUM * 0.8).alias('AUM_DQ')).filter(
            aum_slot_filter.ASS_TYPE == '2').groupBy('CUST_NO').sum('AUM_DQ')

        # 定期活期已计算好,sum(AUM_HQ),sum(AUM_DQ)
        j = huoqi_aum.join(dingqi_aum, 'CUST_NO', 'outer')
        # j.show()


        # 清除原有数据
        self.mysql_helper.execute('truncate core.t_CMMS_ANALYSE_VALUE')

        # 开始联合其他表
        all_col = j.join(cust_info, 'CUST_NO', 'outer')

        print(j.count(), cust_info.count())

        # all_col.show()


        def calculate_rank(value):
            if value < 1000:
                return 0
            elif value < 10000:
                return 1
            elif value < 100000:
                return 2
            elif value < 500000:
                return 3
            elif value < 2000000:
                return 4
            elif value < 5000000:
                return 5
            else:
                return 6

        temp = []
        print('将数据replace到正式表...')
        update_value_sql = "replace into t_CMMS_ANALYSE_VALUE(CUST_ID,CUST_NO,CUST_NM,CUST_VALUE,CUST_RANK,SLOT,UPDATE_TIME) values(%s,%s,%s,%s,%s,%s,now())"
        for row in all_col.collect():

            if len(temp) >= 1000:
                self.mysql_helper.executemany(update_value_sql, temp)
                temp.clear()

            val_dq = row['sum(AUM_DQ)'] if row['sum(AUM_DQ)'] is not None else 0
            val_hq = row['sum(AUM_HQ)'] if row['sum(AUM_HQ)'] is not None else 0

            cust_val = float(val_dq) + float(val_hq)

            cust_rank = calculate_rank(cust_val)

            slot = str(year) + '-' + str(half_year)
            cust_id = row['CUST_ID'] if row['CUST_ID'] is not None else 1
            temp.append((cust_id, row['CUST_NO'], row['CUST_NAM'], cust_val, cust_rank, slot))

        if len(temp) != 1000:
            self.mysql_helper.executemany(update_value_sql, temp)
            temp.clear()

    def aum_total(self):
        '''
        data for t_CMMS_ASSLIB_ASSTOT
        :return:
        '''
        print('---------------------------总资产-Start--------------------------')
        df_asset = self.load_from_mysql('t_CMMS_ASSLIB_ASSET_c').select('CUST_NO', 'CUST_ID', 'STAT_DAT', 'AUM', 'CUR',
                                                                      'ACC_NAM').cache()
        # print(df_asset.count(), df_asset.columns)

        other_col = df_asset.select('CUST_NO', 'CUST_ID', 'CUR', 'ACC_NAM').distinct()
        # print(other_col.count(),other_col.columns)

        aum = df_asset.select('CUST_NO', 'STAT_DAT', 'AUM')
        # print(aum.count(), aum.columns)

        aum = aum.select('CUST_NO', 'STAT_DAT', 'AUM').groupBy(['CUST_NO', 'STAT_DAT']).sum('AUM').sort(
            ['CUST_NO', aum.STAT_DAT.desc()]) \
            .groupBy('CUST_NO').agg({'sum(AUM)': 'first', 'STAT_DAT': 'first'})
        # print(aum.count(), aum.columns)


        total = aum.select('CUST_NO', aum['first(sum(AUM))'].alias('AUM'), aum['first(STAT_DAT)'].alias('STAT_DAT')). \
            join(other_col, 'CUST_NO', 'left_outer').distinct()

        # total.filter(total.STAT_DAT == '2016-06-') .show()

        # prepare params
        def list_map(line):
            return line['CUST_ID'], line['CUST_NO'], line['ACC_NAM'], line['STAT_DAT'], line['CUR'], line['AUM']

        df = total.map(list_map)

        # clear old data
        self.mysql_helper.execute('truncate t_CMMS_ASSLIB_ASSTOT')
        sql = "insert into t_CMMS_ASSLIB_ASSTOT(CUST_ID,CUST_NO,ACC_NAM,STAT_DAT,CUR,AUM) values(%s,%s,%s,%s,%s,%s)"

        # execute sql
        self.sql_operate(sql, df, 100)

    def debt_total(self):
        '''
        prepare data for total debt
        :return:
        '''

        print('---------------------------总负债-Start--------------------------')
        df_debt = self.load_from_mysql('t_CMMS_ASSLIB_DEBT').select('LOAN_ACC', 'CUST_NO', 'CUST_ID', 'CUST_NAM',
                                                                    'BAL_AMT', 'GRANT_AMT', 'CUR')
        df_debt = df_debt.filter(df_debt.LOAN_ACC != '')

        df_sum = df_debt.groupBy('CUST_NO').sum('GRANT_AMT', 'BAL_AMT')
        df_other = df_debt.groupBy('CUST_NO').agg({'CUST_ID': 'first', 'CUST_NAM': 'first', 'CUR': 'first'})

        df_total = df_sum.join(df_other, 'CUST_NO', 'left_outer').distinct()

        stat_dat = datetime.datetime.now().strftime('%Y%m%d')

        def m(line):
            return line['CUST_NO'], line['first(CUST_ID)'], line['first(CUST_NAM)'], line['first(CUR)'], line[
                'sum(GRANT_AMT)'], line['sum(BAL_AMT)'], stat_dat

        df = df_total.map(m)

        sql = "replace into t_CMMS_ASSLIB_DEBTOT(CUST_NO,CUST_ID,ACC_NAM,CUR,LOAN_AMT,BAL_AMT,STAT_DAT) values(%s,%s,%s,%s,%s,%s,%s)"

        self.sql_operate(sql, df)







    def test(self):
        df_asset = self.load_from_mysql('t_CMMS_ASSLIB_ASSET_c').select('CUST_NO', 'CUST_ID', 'STAT_DAT', 'AUM', 'CUR',
                                                                      'ACC_NAM').cache()
        other_col1 = df_asset.select('CUST_NO', 'CUST_ID', 'CUR', 'ACC_NAM').groupBy('CUST_NO').agg(
            {'CUST_NO': 'first', 'CUST_ID': 'first', 'CUR': 'first', 'ACC_NAM': 'first'})
        other_col2 = df_asset.select('CUST_NO', 'CUST_ID', 'CUR', 'ACC_NAM').distinct()
        print(other_col1.count(), other_col2.count())



    def  run(self):
        # 生命周期 年份 季度1,2,3,4
        dh.prepare_life_cycle(2016, 2)

        # 客户价值 上半年：0,下半年：1
        dh.customer_value(2016, 0)

        # 总资产
        dh.aum_total()

        # 总负债
        dh.debt_total()

if __name__ == '__main__':
    dh = DataHandler()

    dh.cycle_credit()


    # dh.cycle_credit()
    # dh.test()
