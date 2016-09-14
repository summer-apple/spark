import pydevd
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.recommendation import *
from numpy import array
import numpy as np
import matplotlib.pyplot as plt


# pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)




class Recommend:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("Summer")
                     .set("spark.cores.max", "2")
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)

        self.base = "hdfs://master:9000/gmc/"

    def papa(self):
        user_data = self.sc.textFile(self.base + 'ml-100k/u.user')
        user_fields = user_data.map(lambda x: x.split('|'))
        num_users = user_fields.map(lambda x: x[0]).count()
        num_genders = user_fields.map(lambda x: x[2]).distinct().count()
        num_occupations = user_fields.map(lambda x: x[2]).distinct().count()
        num_zipcodes = user_fields.map(lambda x: x[4]).distinct().count()
        print("Users: %d, genders: %d, occupations: %d, ZIP codes: %d" % (
            num_users, num_genders, num_occupations, num_zipcodes))

        ages = user_fields.map(lambda x: int(x[1])).collect()

        # plt.hist(ages, bins=20,color='lightblue', normed=True)
        # fig = plt.gcf()
        # fig.set_size_inches(16, 10)
        # plt.show()

        count_by_occupations = user_fields.map(lambda x: (x[3], 1)).reduceByKey(lambda x, y: x + y).collect()
        x_axis1 = np.array(c[0] for c in count_by_occupations)
        y_axis1 = np.array(c[1] for c in count_by_occupations)

        x_axis = x_axis1[np.argsort(y_axis1)]
        y_axis = y_axis1[np.argsort(y_axis1)]

        pos = np.arange(len(x_axis))
        width = 1.0

        ax = plt.axes()
        ax.set_xticks(pos=(width / 2))
        ax.set_xticklabels(x_axis)

        plt.bar(pos, y_axis, width, color='lightblue')
        plt.xticks(rotation=30)
        fig = plt.gcf()
        fig.set_size_inches(16, 10)
        plt.show()

    def prepare(self):
        rate_data = self.sc.textFile(self.base + 'ml-100k/u.data').map(lambda x: x.split('\t'))
        rate_rdd = rate_data.map(lambda x: Rating(float(x[0]), float(x[1]), float(x[2])))
        user_data = self.sc.textFile(self.base + 'ml-100k/u.user')
        movie_data = self.sc.textFile(self.base + 'ml-100k/u.item')

        rate_filter = rate_data.filter(lambda x: x[0] == '1').collect()
        for rf in rate_filter:
            print(rf)

        model = ALS.train(rate_rdd, 10)
        model.save(self.sc, self.base + 'movie_als_model')

        result = model.recommendProducts(1, 10)
        for r in result:
            print(r)


if __name__ == '__main__':
    r = Recommend()
    r.prepare()
    pass
