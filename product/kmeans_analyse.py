import pydevd
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import Vectors
import math
import decimal
import os
try:
    from spark_singleton import SparkEnvirnment
except ImportError:
    import sys, os

    sys.path.append(os.path.abspath('../'))
    from product.spark_env import SparkEnvirnment

# pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)



class KMAnalyse:
    def __init__(self):
        self.spark = SparkEnvirnment(app_name='KMeans', max_cores=4)

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

    def try_different_k(self, dataframe, max_k, min_k=2):
        data = self.prepare_data(dataframe)

        get_id_sql = "select ID from t_CMMS_TEMP_KMEANS_RESULT order by ID desc limit 1"
        try:
            id = int(self.spark.mysql_helper.fetchone(get_id_sql)[0]) + 1
        except:
            id = 1
        columns = str(dataframe.columns)
        l = []
        for k in range(min_k, max_k):
            sorce = self.clustering_score(data, k)
            print(k, sorce)
            l.append(sorce)
            self.spark.mysql_helper.execute('insert into t_CMMS_TEMP_KMEANS_RESULT(ID,K,SORCE,COLUMNS) values(%s,%s,%s,%s)', (id, k, sorce, columns))
        return id


    def prepare_data(self, dataframe):
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

    def train_model(self, dataframe, k, model_name):
        '''
        use data to train model
        :param dataframe: all columns for train
        :param k:k value
        :param model_name:the trained model
        :return:None
        '''

        data = self.prepare_data(dataframe)

        # train to get model
        model = KMeans.train(data, k)

        # create model saving path
        path = self.spark.hdfs_base + model_name

        # try to delete the old model if it exists
        if os.system('hadoop fs -test -e ' + path) == 0:
            os.system('hadoop fs -rm -r ' + path)

        # save new model on hdfs
        model.save(self.spark.sc, path)
        # print all cluster of the model
        for c in model.clusterCenters:
            l = []
            for i in c:
                i = decimal.Decimal(i).quantize(decimal.Decimal('0.01'))
                l.append(float(i))
            print(l)





    def predict(self, model_name, data):

        '''
        predict unknown data
        :param model_name: the trained model saving on hdfs
        :param data: unknown data
        :return: (cluster_index, cluster)
        '''

        # try to load the specified model
        path = self.spark.hdfs_base + model_name
        try:
            model = KMeansModel.load(self.spark.sc, path)
        except:
            raise Exception('No such model found on hdfs!')

        # get the predict : means which cluster it belongs to
        index = model.predict(data)
        print('Data:%s belongs to cluster:%s. The index is %s' % (data, model.clusterCenters[index], index))
        return index, model.clusterCenters[index]

    def vaildate(self, validate_data, model_name):

        correct = 0
        error = 0

        for line in validate_data:
            known_cluster = line[0]
            stay_predict_data = line[1]
            predict_cluster = self.predict(model_name, stay_predict_data)[0]  # Only get the index
            if known_cluster == predict_cluster:
                correct += 1
            else:
                error += 1

        total = len(validate_data)
        result = {'total': total, 'correct': correct, 'error': error, 'accurancy': correct / total}

        print(result)
        return result

    def find_best_k(self, df, times, min_k=2, max_k=15):
        l = []

        for i in range(times):
            id = kma.try_different_k(df, max_k)
            l.append(id)

        sql = "SELECT k,avg(SORCE) FROM t_CMMS_TEMP_KMEANS_RESULT  where ID in %s group by K" % str(tuple(l))
        result = self.spark.mysql_helper.fetchall(sql)
        for i in result:
            print(i[0])
        print('\n')
        for i in result:
            print(i[1])


    def print_model(self,model_name):
        # try to load the specified model
        path = self.spark.hdfs_base + model_name
        try:
            model = KMeansModel.load(self.spark.sc, path)
        except:
            raise Exception('No such model found on hdfs!')

        for c in model.clusterCenters:
            print(c)
        for c in model.clusterCenters:
            l = []
            for i in c:
                i = decimal.Decimal(i).quantize(decimal.Decimal('0.01'))
                l.append(float(i))
            print(l)

    def test_rfm_data(self):
        life_cycle = self.spark.load_from_mysql('t_CMMS_ANALYSE_LIFE').select('CUST_NO', 'LIFE_CYC')
        value = self.spark.load_from_mysql('t_CMMS_ANALYSE_VALUE').select('CUST_NO', 'CUST_VALUE')
        loyalty = self.spark.load_from_mysql('t_CMMS_ANALYSE_LOYALTY').select('CUST_NO', 'LOYALTY')
        rfm = loyalty.join(value, 'CUST_NO', 'left_outer').join(life_cycle, 'CUST_NO', 'left_outer').select('LIFE_CYC',
                                                                                                            'CUST_VALUE',
                                                                                                            'LOYALTY')

        return rfm

    def test_cust_info_data(self):
        return self.spark.load_from_mysql('t_CMMS_TEMP_KMEANS_COLUMNS').select('LIFE_CYC', 'LOYALTY', 'CUST_RANK', 'AGE',
                                                                         'LOCAL', 'SEX', 'AUM')

    def test_credit_data(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').select('CREDIT', 'CYCLE_TIMES', 'CYCLE_AMT',
                                                                        'INSTALLMENT_TIMES', 'INSTALLMENT_AMT',
                                                                        'SWIPE_TIMES', 'SWIPE_AMT', 'CASH_TIMES',
                                                                        'CASH_AMT')

    def full_keys_data(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').select('CREDIT', 'CYCLE_TIMES', 'CYCLE_AMT', 'CYCLE_RATE',
                                                                        'INSTALLMENT_TIMES', 'INSTALLMENT_AMT',
                                                                        'SWIPE_TIMES', 'SWIPE_AMT', 'CASH_TIMES',
                                                                        'CASH_AMT')

    def full_keys_data_credit_less_than_10000(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').filter('CREDIT <= 10000').select('CREDIT', 'CYCLE_TIMES', 'CYCLE_AMT', 'CYCLE_RATE',
                                                                 'INSTALLMENT_TIMES', 'INSTALLMENT_AMT',
                                                                 'SWIPE_TIMES', 'SWIPE_AMT', 'CASH_TIMES',
                                                                 'CASH_AMT')

    def full_keys_data_credit_less_10000_20000(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').filter('CREDIT >= 10000').filter('CREDIT < 20000').select('CREDIT', 'CYCLE_TIMES',
                                                                                                'CYCLE_AMT',
                                                                                                'CYCLE_RATE',
                                                                                                'INSTALLMENT_TIMES',
                                                                                                'INSTALLMENT_AMT',
                                                                                                'SWIPE_TIMES',
                                                                                                'SWIPE_AMT',
                                                                                                'CASH_TIMES',
                                                                                                'CASH_AMT')

    def full_keys_data_credit_more_than_20000(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').filter('CREDIT >= 20000').select('CREDIT', 'CYCLE_TIMES',
                                                                                                'CYCLE_AMT',
                                                                                                'CYCLE_RATE',
                                                                                                'INSTALLMENT_TIMES',
                                                                                                'INSTALLMENT_AMT',
                                                                                                'SWIPE_TIMES',
                                                                                                'SWIPE_AMT',
                                                                                                'CASH_TIMES',
                                                                                                'CASH_AMT')

    def no_cycle_data(self):
        return self.spark.spark.load_from_mysql('t_CMMS_CREDIT_STAT').select('CREDIT',
                                                                        'INSTALLMENT_TIMES', 'INSTALLMENT_AMT',
                                                                        'SWIPE_TIMES', 'SWIPE_AMT', 'CASH_TIMES',
                                                                        'CASH_AMT')
    def no_amt_data(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').select('CREDIT', 'CYCLE_TIMES',
                                                                        'CYCLE_RATE',
                                                                        'INSTALLMENT_TIMES',
                                                                        'SWIPE_TIMES',  'CASH_TIMES')

    def no_cycle_amt(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').select('CREDIT', 'CYCLE_TIMES',
                                                                        'CYCLE_RATE',
                                                                        'INSTALLMENT_TIMES', 'INSTALLMENT_AMT',
                                                                        'SWIPE_TIMES', 'SWIPE_AMT', 'CASH_TIMES',
                                                                        'CASH_AMT')

    def only_swipe(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').select('CYCLE_TIMES',
                                                                 'CYCLE_RATE',
                                                                 'INSTALLMENT_TIMES',
                                                                 'SWIPE_TIMES', 'SWIPE_AMT', 'CASH_TIMES')

    def no_zero_data(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').filter('CYCLE_TIMES = 0 and INSTALLMENT_TIMES = 0 and SWIPE_TIMES = 0 and CASH_TIMES = 0')


    def full_key(self):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').filter('CREDIT > 5000').filter('CREDIT <= 10000').subtract(self.no_zero_data()).select('CREDIT', 'CYCLE_TIMES',
                                                                                                'CYCLE_AMT',
                                                                                                'CYCLE_RATE',
                                                                                                'INSTALLMENT_TIMES',
                                                                                                'INSTALLMENT_AMT',
                                                                                                'SWIPE_TIMES',
                                                                                                'SWIPE_AMT',
                                                                                                'CASH_TIMES',
                                                                                                'CASH_AMT')

    def full_key(self,min,max):
        return self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').filter('CREDIT > '+str(min)).filter('CREDIT <= '+str(max)).subtract(self.no_zero_data()).select('CREDIT', 'CYCLE_TIMES',
                                                                                                'CYCLE_AMT',
                                                                                                'CYCLE_RATE',
                                                                                                'INSTALLMENT_TIMES',
                                                                                                'INSTALLMENT_AMT',
                                                                                                'SWIPE_TIMES',
                                                                                                'SWIPE_AMT',
                                                                                                'CASH_TIMES',
                                                                                                'CASH_AMT')

if __name__ == '__main__':
    kma = KMAnalyse()
    df = kma.full_key(1000,20000)
    #kma.find_best_k(df,10,max_k=10)


    kma.train_model(df, 7, 'full_keys_data')

    # print()
    # kma.train_model(df, 8, 'full_keys_data')
    # print()
    # kma.train_model(df, 9, 'full_keys_data')
    # print()
    # kma.train_model(df, 10, 'full_keys_data')

    # kma.predict('all_col_kmeans_k4',[1, 2, 2,27,330621,1,20000])



