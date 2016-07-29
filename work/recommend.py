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

        base = "hdfs://master:9000/gmc/"
        self.rawUserArtistData = self.sc.textFile(base + "user_artist_data.txt")
        self.rawArtistData = self.sc.textFile(base + "artist_data.txt")
        self.rawArtistAlias = self.sc.textFile(base + "artist_alias.txt")







    def buildArtistByID(self,rawArtistData):

        def buildArtistByIDFlatMap(line):
            id_name = line.split(' ', 1)
            id = id_name[0]
            if len(id_name) > 1:
                name = id_name[1]
            else:
                return None

            try:
                id = int(id)
                return id, str.strip(name)
            except Exception:
                return None

        return rawArtistData.flatMap(buildArtistByIDFlatMap)

    def buildArtistAlias(self,rawArtistAlias):

        def buildArtistAliasFlatMap(line):
            ids = line.split(' ',1)
            try:
                id_1 = int(str.strip(ids[0]))
                id_2 = int(str.strip(ids[1]))
                return id_1,id_2
            except Exception:
                return None

        return rawArtistAlias.flatMap(buildArtistAliasFlatMap)

    def test(self):
        artist_by_id = self.buildArtistByID(self.rawArtistData)
        #artist_alias = self.buildArtistAlias(self.rawArtistAlias)

        a = artist_by_id.lookup('6803336').head()
        b = artist_by_id.lookup('1000010').head()

        print(a)
        print(b)








if __name__ == '__main__':
    #r = Recommend()
    #r.test()
    print('fuck you')




