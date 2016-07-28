import pydevd
from pyspark import SparkContext,SparkConf,SQLContext
pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)




class Recommend:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("Summer")
                     .set("spark.cores.max", "2")
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)

        base = "hdfs:///user/ds/"
        self.rawUserArtistData = self.sc.textFile(base + "user_artist_data.txt")
        self.rawArtistData = self.sc.textFile(base + "artist_data.txt")
        self.rawArtistAlias = self.sc.textFile(base + "artist_alias.txt")


    def buildArtistByIDFlatMap(self,line):
        kv = line.split('/t')
        k = kv[0]
        v = kv[1]

        if v is None:
            return None
        else:
            try:
                k = int(k)
                return ((k,v))
            except Exception:
                return None




    def buildArtistByID(self,row):
        row.flatMap(lambda line: )

    def test(self):
        pass





if __name__ == '__main__':
    r = Recommend()
    r.test()




