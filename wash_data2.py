import re
import pydevd
from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.sql.dataframe import DataFrame
from pyspark.mllib.clustering import KMeans
import sys
from math import sqrt
from numpy import array



pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)



conf = (SparkConf()
                .setAppName("Summer")
                .set("spark.cores.max", "2")
                .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def load_from_mysql(database,table):
    url = "jdbc:mysql://10.9.29.212:3306/%s?user=root&characterEncoding=UTF-8" % database
    df = sqlContext.read.format("jdbc").options(url=url, dbtable=table, driver="com.mysql.jdbc.Driver").load()
    return df


def initData():
        # BFFMDQAB 定期账户分户--单张存折信息
        # BDFMHQBC 开户登记簿--存折与客户关联信息
        # BDFMHQAB 活期账户余额

        # 获取定期存款表
    deposit = load_from_mysql('core','BFFMDQAB')
        # 获取账户表
    account = load_from_mysql('core','BDFMHQBC')
        # 两表JOIN
    j = deposit.join(account,deposit.AB01AC15 == account.BC05AC15)

    sqlContext.registerDataFrameAsTable(deposit,'deposit')
    sqlContext.registerDataFrameAsTable(account,'account')

    j.select('AB01AC15').show()






    '''
    返回活期存款关键信息:
    客户内码--持卡数量--所有账户总金额--日期
    +-----------+----------+-----------+-----------+
    |customer_id|card_count|money_total|update_date|
    +-----------+----------+-----------+-----------+
    |81067636619|         3|      27911| 2016-05-30|
    |81036114513|         1|        110| 2016-05-30|
    |81036281509|         1|      11359| 2016-05-30|
    |81042207870|         1|         51| 2016-05-30|
    |81022212397|         3|    1249392| 2016-05-30|
    +-----------+----------+-----------+-----------+
    '''
def initDemandDepositData():
    demand_deposit = load_from_mysql('core','BDFMHQAB_D')
    sqlContext.registerDataFrameAsTable(demand_deposit,'demand_deposit')
        #demand_deposit.select('*').show()
        #demand_deposit.select('AB05CSNO','UPDTDATE').groupBy('AB05CSNO','UPDTDATE').count().show()
    sql = "SELECT AB05CSNO AS customer_id,COUNT(*) AS card_count,SUM(AB16BAL) AS money_total,UPDTDATE AS update_date FROM " \
              "demand_deposit GROUP BY AB05CSNO,UPDTDATE"
    sample_demand_deposit = sqlContext.sql(sql)
        #sample_demand_deposit.select('*').show()
    return sample_demand_deposit


    '''
    定期存款数据筛选:
    客户内码--存折数量--总金额--总存期--平均存期--时间
    +-----------+----------+-----------+----------+--------+-----------+
    |customer_id|card_count|money_total|time_total|time_avg|update_date|
    +-----------+----------+-----------+----------+--------+-----------+
    |81029469226|         1|       5019|         3|       3| 2016-05-30|
    |81043782206|         2|    1370000|        14|       7| 2016-05-30|
    |81064173489|         1|      10080|         3|       3| 2016-05-30|
    |81022817150|         1|      11000|        12|      12| 2016-05-30|
    |81043782206|         2|    1370000|        14|       7| 2016-05-30|
    |81022025273|         2|      67703|        24|      12| 2016-05-30|
    +-----------+----------+-----------+----------+--------+-----------+
    '''
def initDepositData():
    deposit = load_from_mysql('core','BFFMDQAB_D')
    customer_info = load_from_mysql('core','BCFMCMBI_D')

    sqlContext.registerDataFrameAsTable(deposit,'deposit')
    sqlContext.registerDataFrameAsTable(customer_info,'customer_info')

    sql = "SELECT d.AB08CSNO AS customer_id," \
              "c.CUIDCSID AS idcard," \
              "COUNT(d.AB01AC15) AS card_count," \
              "SUM(d.AB21BAL) AS money_total," \
              "SUM(d.AB16SN03) AS time_total," \
              "floor((SUM(d.AB16SN03)/COUNT(d.AB01AC15))) AS time_avg, " \
              "d.UPDTDATE AS update_date " \
              "FROM deposit AS d JOIN customer_info AS c ON d.AB08CSNO=c.CINOCSNO " \
              "GROUP BY d.AB08CSNO,c.CUIDCSID,d.UPDTDATE"

    sample_deposit = sqlContext.sql(sql)
        # sample_deposit.select('*').show()
    return sample_deposit

    '''
    日AUM:合并同一用户,同一日期的活期和定期存款
    +-----------+-----------+-----------+
    |customer_id|money_total|update_date|
    +-----------+-----------+-----------+
    |81028821120|       4660| 2016-05-30|
    |81032371639|     401047| 2016-05-30|
    |81048159014|     162149| 2016-05-30|
    |81048692775|     102697| 2016-05-30|
    +-----------+-----------+-----------+



    月AUM统计
    +-----------+-----------+-------+
    |customer_id|money_total|  month|
    +-----------+-----------+-------+
    |81022940695|      64590|2016-05|
    |81052891668|       5330|2016-05|
    |81023452967|      29655|2016-05|
    |81043764991|     630028|2016-05|
    +-----------+-----------+-------+
    '''
def initAUMData():
    sample_deposit = initDepositData()
    sample_demand_deposit = initDemandDepositData()

    sqlContext.registerDataFrameAsTable(sample_deposit,'sample_deposit')
    sqlContext.registerDataFrameAsTable(sample_demand_deposit,'sample_demand_deposit')

    sql = 'SELECT sd.customer_id,' \
              'sd.money_total+sdd.money_total AS money_total,' \
              'sd.update_date FROM sample_deposit sd ' \
              'JOIN sample_demand_deposit sdd ' \
              'ON sd.customer_id = sdd.customer_id ' \
              'AND sd.update_date = sdd.update_date'

        #日AUM
    daily_aum = sqlContext.sql(sql)

    sqlContext.registerDataFrameAsTable(daily_aum,'daily_aum')
    sql2 = "SELECT customer_id," \
               "AVG(money_total) AS money_total," \
               "DATE_FORMAT(update_date,'Y-MM') AS month FROM daily_aum " \
               "GROUP BY customer_id,DATE_FORMAT(update_date,'Y-MM') ORDER BY DATE_FORMAT(update_date,'Y-MM') DESC"
        #月日均AUM(此处以已有天数统计,即五天数据则求五天均值)
    month_aum = sqlContext.sql(sql2)


    month_aum.select('*').show()

    return month_aum



def aum_change():
    #month_aum = initAUMData()

    #sqlContext.registerDataFrameAsTable(month_aum,'month_aum')


    cus_info = load_from_mysql('core','BCFMCMBI_D')
    kaihu = load_from_mysql('core','BDFMHQBC_D')

    sqlContext.registerDataFrameAsTable(cus_info,'cusinfo')
    sqlContext.registerDataFrameAsTable(kaihu,'kaihu')





    # sql = "SELECT * FROM " \
    #       "(SELECT kaihu.BC07CSNO AS KHH, " \
    #       "cusinfo.BUDIDATE AS SCKAIHU, " \
    #       "period_diff(date_format(now(),'%Y%m'),DATE_FORMAT(cusinfo.BUDIDATE,'%Y%m')) AS KHSC, " \
    #       "date_add(kaihu.BC31DATE, INTERVAL kaihu.BC40SN03 MONTH) AS DAOQI " \
    #       "FROM kaihu JOIN cusinfo ON kaihu.BC07CSNO = cusinfo.CINOCSNO " \
    #       "ORDER BY DAOQI DESC) AS tmp GROUP BY tmp.KHH"
    #
    # sql = "SELECT kaihu.BC07CSNO AS KHH,cusinfo.BUDIDATE AS SCKAIHU," \
    #       "period_diff(date_format(now(),'%Y%m'),DATE_FORMAT(cusinfo.BUDIDATE,'%Y%m')) AS KHSC," \
    #       "date_add(kaihu.BC31DATE, INTERVAL kaihu.BC40SN03 MONTH) AS DAOQI " \
    #       "FROM kaihu JOIN cusinfo ON kaihu.BC07CSNO = cusinfo.CINOCSNO " \
    #       "ORDER BY DAOQI DESC "

    sql = "SELECT kaihu.BC07CSNO AS KHH," \
          "cusinfo.BUDIDATE AS SCKAIHU, " \
          "" \
          "FROM kaihu JOIN cusinfo ON kaihu.BC07CSNO = cusinfo.CINOCSNO "

    result = sqlContext.sql(sql)

    result.select('*').show()

    return result


initAUMData()