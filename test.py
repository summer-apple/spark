import pydevd
from pyspark import SparkContext,SparkConf,SQLContext




pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)



conf = (SparkConf()
                .setAppName("Summer")
                .set("spark.cores.max", "2")
                .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def load_from_mysql(table):
    url = "jdbc:mysql://10.9.29.212:3306/core?user=root&characterEncoding=UTF-8"
    df = sqlContext.read.format("jdbc").options(url=url, dbtable=table, driver="com.mysql.jdbc.Driver").load()
    return df




aum = load_from_mysql('t_CMMS_ASSLIB_ASSET')

aum.show()

sqlContext.registerDataFrameAsTable(aum,'aum')



