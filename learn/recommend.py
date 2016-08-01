import pydevd
from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.mllib.fpm import FPGrowth,PrefixSpan


pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)




class Recommend:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("Summer")
                     .set("spark.cores.max", "2")
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)

        self.base = "hdfs://master:9000/gmc/"





    def test(self):

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

    def test2(self):
        data = [
            ['A', 'B', 'C'],
            ['A', 'B', 'D'],
            ['A', 'G', 'C'],
            ['A', 'B', 'R'],
            ['H', 'J', 'K']

        ]
        data2 = [1,2,3,4,5,3,2,3,21,4,5,4,5]
        rdd = self.sc.parallelize(data2, 2).cache()
        model = FPGrowth.train(rdd, minSupport=0.3, numPartitions=10)
        result = model.freqItemsets().collect()
        for r in result:
            print(r)


    def test3(self):
        '''
        frequent mining
        :return:
        '''

        data = [
            ['P1', 'P2', 'P3'],
            ['P1', 'P2', 'P3', 'P7'],
            ['P1', 'P3'],
            ['P3', 'P5', 'P4', 'P6'],
            ['P4', 'P5']

        ]
        rdd = self.sc.parallelize(data, 2).cache()
        model = FPGrowth.train(rdd, minSupport=0.3, numPartitions=10)
        result = model.freqItemsets().collect()
        for r in result:
            print(r)





if __name__ == '__main__':
    r = Recommend()
    r.test3()





