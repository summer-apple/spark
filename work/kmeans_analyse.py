import pydevd
from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.linalg import Vectors
import math


pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)




class KMAnalyse:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("KMeans")
                     .set("spark.cores.max", "2")
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)

    def load_from_mysql(self, table, database='core'):
        url = "jdbc:mysql://10.9.29.212:3306/%s?user=root&characterEncoding=UTF-8" % database
        df = self.sqlctx.read.format("jdbc").options(url=url, dbtable=table, driver="com.mysql.jdbc.Driver").load()
        return df



    def perpare_data(self):
        life_cycle = self.load_from_mysql('t_CMMS_ANALYSE_LIFE').select('CUST_NO','LIFE_CYC')
        value = self.load_from_mysql('t_CMMS_ANALYSE_VALUE').select('CUST_NO','CUST_VALUE')
        loyalty = self.load_from_mysql('t_CMMS_ANALYSE_LOYALTY').select('CUST_NO','LOYALTY')

        rfm = loyalty.join(value,'CUST_NO','left_outer').join(life_cycle,'CUST_NO','left_outer')

        return rfm.rdd

    @staticmethod
    def clustering_score(data, k):
        model = KMeans.train(data, k=k)

        def distance(v1, v2):
            s = 0
            # [1,2,3] [4,5,6] --> [(1,4),(2,5),(3,6)]
            pairs = zip(v1, v2)
            for p in pairs:
                sub = float(p[0]) - float(p[1])
                s = s + sub * sub
            return math.sqrt(s)

        def dist_to_centroid(datum):
            # predict the data
            cluster = model.predict(datum)
            # get the current centroid --> means center point
            centroid = model.clusterCenters[cluster]
            # call distance method
            return distance(centroid, datum)

        return data.map(dist_to_centroid).mean()

    def try_k(self):
        #file = self.sc.textFile(self.base + 'k_data.csv')
        #data = file.map(lambda line: line.split(',')).cache()
        data = self.perpare_data()

        def v_map(line):
            lst = []
            for c in line:
                if c is not None:
                    lst.append(float(c))
                else:
                    lst.append(0.0)
            return lst
        data = data.map(v_map)

        print(type(data))
        for k in range(1, 100):
            sorce = self.clustering_score(data, k)
            print(k, sorce)







if __name__ == '__main__':
    kma = KMAnalyse()

    kma.try_k()



