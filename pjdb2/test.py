import pydevd
from pyspark import SparkContext, SparkConf, SQLContext

pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)

conf = (SparkConf()
        .setAppName("Summer")
        .set("spark.cores.max", "12")
        .set('spark.executor.extraClassPath', '/usr/local/env/lib/*'))

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


def load_from_db2(table):
    url = "jdbc:db2://192.168.17.40:50111/XUJIE2"
    driver = 'com.ibm.db2.jcc.DB2Driver'
    df = sqlContext.read.format("jdbc").options(url=url, dbtable=table, user='xuj_db2', password='xuj_db2',
                                                protocol='tcpip', driver=driver).load()
    df.show()
    return df


if __name__ == '__main__':
    df = load_from_mysql('T_POINT_SYS')
    print(df.count())
    print(df.columns)
    df.show()
