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



        #计算月份

        if season == 1:
            #date1 当前季度月份
            date1 = [str(year)+'-01',str(year)+'-02',str(year)+'-03']

            #date2 上一季月份
            date2 = [str(year-1) + '-10', str(year-1) + '-11', str(year-1) + '-12']

        elif season == 4:
            date1 = [str(year) + '-10', str(year) + '-11', str(year) + '-12']
            date2 = [str(year) + '-07', str(year) + '-08', str(year) + '-9']

        else:
            date1 = [str(year) + '-0'+str(3*season-2), str(year) + '-0'+str(3*season-1), str(year) + '-0'+str(3*season)]
            date2 = [str(year) + '-0'+str(3*season-5), str(year) + '-0'+str(3*season-4), str(year) + '-0'+str(3*season-3)]


        print('当前季度月份：',date1)
        print('上一季度月份：',date2)










        #加载AUM表
        aum = self.load_from_mysql('t_CMMS_ASSLIB_ASSET').cache()


        #拼接每季度三个月断数据
        season_new = aum.filter(aum.STAT_DAT == date1[0]).unionAll(aum.filter(aum.STAT_DAT == date1[1])).unionAll(aum.filter(aum.STAT_DAT == date1[2]))
        season_old = aum.filter(aum.STAT_DAT == date2[0]).unionAll(aum.filter(aum.STAT_DAT == date2[1])).unionAll(aum.filter(aum.STAT_DAT == date2[2]))

        #计算每季度AUM
        aum_season_old = season_old.select('CUST_NO', season_old.AUM.alias('AUM1')).groupBy('CUST_NO').sum('AUM1')
        aum_season_new = season_new.select('CUST_NO', season_new.AUM.alias('AUM2')).groupBy('CUST_NO').sum('AUM2')


        #两个季度进行外联接
        '''
        +-----------+---------+---------+
        |    CUST_NO|sum(AUM2)|sum(AUM1)|
        +-----------+---------+---------+
        |81005329523|     null|294844.59|
        |81011793167|     null|   365.20|
        |81015319088|     null|  9640.96|
        +-----------+---------+---------+
        '''
        union_season = aum_season_old.join(aum_season_new,'CUST_NO','outer')







        # 筛选当前AUM
        temp_result = aum.select('CUST_NO', 'AUM', 'STAT_DAT').groupBy('CUST_NO', 'STAT_DAT').sum('AUM').sort(
            'CUST_NO').sort(aum.STAT_DAT.desc())
        temp_result.select('CUST_NO', temp_result['sum(AUM)'].alias('AUM'), 'STAT_DAT').registerTempTable('group_in')

        aum_now_sql = "select CUST_NO,first(AUM) as AUM_NOW from group_in group by CUST_NO"

        aum_now = self.sqlctx.sql(aum_now_sql)
        # 清除缓存表
        self.sqlctx.dropTempTable('group_in')

        # 联合
        union_season_aumnow = union_season.join(aum_now,'CUST_NO','outer')









        # 计算用户开户至今时间
        # 载入账户表
        self.load_from_mysql('t_CMMS_ACCOUNT_LIST').registerTempTable('account')
        account_age_aql = "select  CUST_NO, first(ACCOUNT_AGE) as ACCOUNT_AGE  from " \
                          "(select CUST_NO, round(datediff(now(), OPEN_DAT) / 30) as ACCOUNT_AGE " \
                          "from account order by CUST_NO, ACCOUNT_AGE desc ) as t group by CUST_NO"

        account_age = self.sqlctx.sql(account_age_aql)


        # 联合
        union_season_aumnow_accountage = union_season_aumnow.join(account_age,'CUST_NO','outer')

        # 清除缓存表
        self.sqlctx.dropTempTable('account')
        self.sqlctx.clearCache()











        #结果插入表

        insert_lifecycle_sql = "replace into t_CMMS_TEMP_LIFECYCLE(CUST_NO,SAUM1,SAUM2,INCREASE,ACCOUNT_AGE,AUM_NOW) values(%s,%s,%s,%s,%s,%s)"

        #缓冲区
        temp = []
        for row in union_season_aumnow_accountage.collect():
            row_dic = row.asDict()

            if len(temp) >= 1000:#批量写入数据库
                self.mysql_helper.executemany(insert_lifecycle_sql, temp)
                temp.clear()

            #加载数据到缓冲区

            try:
                increase = (row_dic['sum(AUM2)'] - row_dic['sum(AUM1)']) / row_dic['sum(AUM1)']
            except Exception:
                increase = 0

            if row_dic['ACCOUNT_AGE'] is None:
                row_dic['ACCOUNT_AGE'] = 7

            temp.append((row_dic['CUST_NO'], row_dic['sum(AUM1)'], row_dic['sum(AUM2)'],increase,row_dic['ACCOUNT_AGE'],row_dic['AUM_NOW']))


        if len(temp) != 0:
            self.mysql_helper.executemany(insert_lifecycle_sql, temp)
            temp.clear()






    def test(self):



        pass





if __name__ == '__main__':
    dh = DataHandler()
    dh.life_cycle(2016, 2)
    #dh.test()