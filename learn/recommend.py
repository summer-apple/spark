import pydevd
from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.mllib.fpm import FPGrowth,PrefixSpan
from pyspark.mllib.clustering import KMeans,KMeansModel
import math

#pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)




class Recommend:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("Summer")
                     .set("spark.cores.max", "2")
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)

        self.base = "hdfs://master:9000/gmc/"





    def gp_growth_demo(self):

        '''
        r z h k p
        z y x w v u t s
        s x o n r
        x z y m t s q e
        z
        x z y r q t p
        :return:
        '''
        data = self.sc.textFile(self.base + 'sample_fpgrowth.txt')
        transactions = data.map(lambda line: line.strip().split(' '))
        model = FPGrowth.train(transactions, minSupport=0.3, numPartitions=10)
        result = model.freqItemsets().collect()
        for fi in result:
            print(fi)




    def fpgrowth(self):
        '''
        frequent mining

        1.get the group of similar customers
        2.list the products these customers using
        3.run this fpgroup
        :return:
        '''

        data = [
            ['1', '1', '2'],
            ['2', '1', '1', '2'],
            ['P1', 'P3'],
            ['P3', 'P5', 'P4', 'P6'],
            ['P4', 'P5']
        ]
        rdd = self.sc.parallelize(data, 2).cache()
        model = FPGrowth.train(rdd, minSupport=0.3, numPartitions=10)
        result = model.freqItemsets().collect()
        for r in result:
            print(r)




    def kmeans_demo(self):

        file = self.sc.textFile(self.base+'k_data.csv')

        # transform to rdd
        data = file.map(lambda line: line.split(',')).cache()
        print(type(data))

        # train data to get the model
        model = KMeans.train(data,k=3)

        # print to check all clusters
        cluster = model.clusterCenters
        for c in cluster:
            print(c)


        # predict new data  return the data belong to which cluster(index of the cluster)
        predict = model.predict([1.3,.1,1.1])

        print(predict)


    # calculate the distance of data point to cluster center
    # @staticmethod
    # def distance(v1,v2):
    #     sum = 0
    #     for a, b in v1, v2:
    #         sum = sum + (a-b)*(a-b)
    #
    #     return math.sqtr(sum)


    # the distance to
    # @staticmethod
    # def dist_to_centroid(datum,model):
    #     # predict the data
    #     cluster = model.predict(datum)
    #     # get the current centroid --> means center point
    #     centroid = model.clusterCenters(cluster)
    #     # call distance method
    #     self.distance(centroid,datum)

    @staticmethod
    def clustering_score(data,k):
        model = KMeans.train(data, k=k,maxIterations=200)

        def distance(v1, v2):
            s = 0
            # [1,2,3] [4,5,6] --> [(1,4),(2,5),(3,6)]
            pairs = zip(v1,v2)
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
        file = self.sc.textFile(self.base + 'k_data.csv')
        data = file.map(lambda line: line.split(',')).cache()
        print(data.count())
        for k in range(100,200):
            sorce = self.clustering_score(data,k)
            print(k,sorce)


if __name__ == '__main__':
    r = Recommend()
    #r.try_k()

    r.fpgrowth()





