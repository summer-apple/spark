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




'''
1 24676497.033871297
2 8951761.036445381
3 5659909.243875426
4 3544142.668226625
5 2569484.7180111054
6 2503238.746591233
7 2236166.756611932
8 1192086.322984488
9 1286045.6170509097
10 1139246.1954498296
11 941970.3719946971
12 938812.9030484511
13 786332.4741078989
14 739536.349951354
15 682090.7420476933
16 671403.9108427719
17 661861.6621661095
18 589244.7939772053
19 620507.0306452995
20 578411.2500735886
21 535323.4772133254
22 506451.4268414376
23 487216.11703516694
24 506179.81070595625
25 470920.8930468655
26 486354.4356718584
27 437297.78198168345
28 411169.2001897343
29 447884.51890211453
30 400038.37464469066
'''
