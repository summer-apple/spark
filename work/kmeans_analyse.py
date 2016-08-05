import pydevd
from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.mllib.clustering import KMeans,KMeansModel
from pyspark.mllib.linalg import Vectors
import math
try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys,os
    sys.path.append(os.path.abspath('../'))
    from work.mysql_helper import MySQLHelper


#pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)



class KMAnalyse:
    def __init__(self):
        self.conf = (SparkConf()
                     .setAppName("KMeans")
                     .set("spark.cores.max", "2")
                     .set('spark.executor.extraClassPath', '/usr/local/env/lib/mysql-connector-java-5.1.38-bin.jar'))
        self.sc = SparkContext(conf=self.conf)
        self.sqlctx = SQLContext(self.sc)
        self.mysql_helper = MySQLHelper('core', host='10.9.29.212')
        self.base = 'hdfs://master:9000/gmc/'


    def load_from_mysql(self, table, database='core'):
        url = "jdbc:mysql://10.9.29.212:3306/%s?user=root&characterEncoding=UTF-8" % database
        df = self.sqlctx.read.format("jdbc").options(url=url, dbtable=table, driver="com.mysql.jdbc.Driver").load()
        return df



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



    def try_different_k(self,dataframe,stop,start=2):
        data = self.prepare_data(dataframe)

        get_id_sql = "select ID from t_CMMS_TEMP_KMEANS_RESULT order by ID desc limit 1"
        try:
            id = int(self.mysql_helper.fetchone(get_id_sql)[0]) + 1
        except:
            id = 1
        columns = str(dataframe.columns)

        for k in range(start, stop):
            sorce = self.clustering_score(data, k)
            print(k, sorce)
            self.mysql_helper.execute('insert into t_CMMS_TEMP_KMEANS_RESULT(ID,K,SORCE,COLUMNS) values(%s,%s,%s,%s)', (id,k, sorce,columns))


    def prepare_data(self,dataframe):
        '''
        format data
        :param dataframe:
        :return:
        '''

        def v_map(line):
            lst = []
            for c in line:
                if c is not None:
                    lst.append(float(c))
                else:
                    lst.append(0.0)
            return lst

        return dataframe.rdd.cache().map(v_map)



    def train_model(self,dataframe,k,model_name):
        '''
        use data to train model
        :param dataframe: all columns for train
        :param k:k value
        :param model_name:the trained model
        :return:None
        '''

        data = self.prepare_data(dataframe)

        #train to get model
        model = KMeans.train(data,k)

        #create model saving path
        path = self.base + model_name

        #try to delete the old model if it exists
        try:
            import subprocess
            subprocess.call(["hadoop", "fs", "-rm", "-f", path])
        except:
            pass
        # save new model on hdfs
        model.save(self.sc, path)

        #print all cluster of the model
        for c in model.clusterCenters:
            print(c)



    def predict(self,model_name,data):

        '''
        predict unknown data
        :param model_name: the trained model saving on hdfs
        :param data: unknown data
        :return: (cluster_index, cluster)
        '''

        # try to load the specified model
        path = self.base + model_name
        try:
            model = KMeansModel.load(self.sc, path)
        except:
            raise Exception('No such model found on hdfs!')

        #get the predict : means which cluster it belongs to
        index = model.predict(data)
        print('Data:%s belongs to cluster:%s. The index is %s' % (data, model.clusterCenters[index], index))
        return index, model.clusterCenters[index]



    def vaildate(self,validate_data,model_name):

        correct = 0
        error = 0

        for line in validate_data:
            known_cluster = line[0]
            stay_predict_data = line[1]
            predict_cluster = self.predict(model_name,stay_predict_data)[0] # Only get the index
            if known_cluster == predict_cluster:
                correct += 1
            else:
                error += 1

        total = len(validate_data)
        result = {'total':total,'correct':correct,'error':error,'accurancy':correct/total}

        print(result)
        return result


    def find_best_k(self,model_id):
        # TODO find best k
        sql = "select * "

    def test_data(self):
        life_cycle = self.load_from_mysql('t_CMMS_ANALYSE_LIFE').select('CUST_NO', 'LIFE_CYC')
        value = self.load_from_mysql('t_CMMS_ANALYSE_VALUE').select('CUST_NO', 'CUST_VALUE')
        loyalty = self.load_from_mysql('t_CMMS_ANALYSE_LOYALTY').select('CUST_NO', 'LOYALTY')
        rfm = loyalty.join(value, 'CUST_NO', 'left_outer').join(life_cycle, 'CUST_NO', 'left_outer').select('LIFE_CYC', 'CUST_VALUE', 'LOYALTY')

        return rfm


    def test_1(self):
        return self.load_from_mysql('t_CMMS_TEMP_KMEANS_COLUMNS').select('LIFE_CYC','LOYALTY','CUST_RANK','AGE','LOCAL','SEX','AUM')


if __name__ == '__main__':
    kma = KMAnalyse()
    df = kma.test_1()
    kma.try_different_k(df,15)
    #kma.train_model(df, 5, 'rmf_kmeans')
    #kma.predict('rmf_kmeans',[2.28426396e-02, 2.57216490e+05, 1.32690355e+00])








