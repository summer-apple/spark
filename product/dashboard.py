import json

try:
    from spark_env import SparkEnvirnment
except ImportError:
    import sys, os

    sys.path.append(os.path.abspath('../'))
    from product.spark_env import SparkEnvirnment

import time
import datetime

class Dashboard:
    def __init__(self):
        self.spark = SparkEnvirnment()




    def select_user(self,json_str):

        '''

        :param json_str:
        [{
        type: int | list | str
        key: key_name --> column_name
        value: min,max | a,b,c,d | 'string' },
        {...},
        {...}
        ]
        :return:
        '''


        # 信用卡用户信息总表
        # TODO 改为t_CMMS_CREDIT_USERINFO
        df = self.spark.load_from_mysql('t_CMMS_CREDIT_STAT').select('ACCTNBR','CREDIT','SWIPE_TIMES').cache()

        # TODO 暂时写死
        json_str = '[{"type": "int", "value": "5000,10000", "key": "CREDIT"},{"type": "list", "value": "2,4,5,6,13,18", "key": "SWIPE_TIMES"}]'

        # 格式化json参数
        kws_list = json.loads(json_str)

        result = None

        # 遍历参数，针对每个参数进行筛选，然后取交集
        for d in kws_list:
            if result is None:
                result = self.filter(df,d)
            else:
                result = result.intersect(self.filter(df,d))

        print(result.count())
        result.show()


    def filter(self,df,condition_dict):
        '''
        根据参数筛选客户
        :param df: 客户dataframe
        :param condition_dict: 筛选条件
        :return:
        '''
        d = condition_dict

        # 如果是int类型的条件 则做大于 小于 等于判断
        if d['type'] == 'int':
            min = d['value'].split(',')[0]
            max = d['value'].split(',')[1]
            if min == max:
                condition = d['key'] + '=' + str(min)
            else:
                condition = '%s >= %s and %s <= %s' % (d['key'], min, d['key'], max)

            step_result = df.filter(condition)

        # 如果是list类型 则遍历每个值，然后联合取并集
        elif d['type'] == 'list':
            values = d['value'].split(',')
            step_result = None
            for v in values:
                condition = d['key'] + '=' + v
                if step_result is None:
                    step_result = df.filter(condition)
                else:
                    step_result = step_result.unionAll(df.filter(condition))

        # 如果是字符串 视为精确搜索
        elif d['type'] == 'str':
            condition = d['key'] + '=' + d['value']
            step_result = df.filter(condition)

        step_result.show()
        print(step_result.count())
        return step_result


    def recommend_user(self, aim):
        pass



    def brief_info_stat(self, month_nbr):
        month = datetime.datetime.now().month
        # TODO 明确具体要统计的信息



















if __name__ == '__main__':
    d = Dashboard()
    t1 = time.time()
    d.select_user('')
    t2 = time.time()

    delt = t2-t1
    print(delt)