import pydevd
from pyspark.sql.types import Row
from pyspark.mllib.fpm import FPGrowth
import datetime
try:
    from spark_env import SparkEnvirnment
except ImportError:
    import sys, os

    sys.path.append(os.path.abspath('../'))
    from product.spark_env import SparkEnvirnment

#pydevd.settrace("60.191.25.130", port=8618, stdoutToServer=True, stderrToServer=True)


class Rule:
    def __init__(self, rule_id, rule_name, source_table, point_rule, cycle, state, clazz, version, desp, create_time, remark1, remark2, props=None):
        self.rule_id = rule_id
        self.rule_name = rule_name
        self.source_table = source_table
        self.point_rule = point_rule
        self.cycle = cycle
        self.state = state
        self.clazz = clazz
        self.version = version
        self.desp = desp
        self.create_time = create_time
        self.remark1 = remark1
        self.remark2 = remark2
        self.props = props


    def __str__(self):
        print('RuleID:', self.rule_id, 'RuleName:', self.rule_name)

class Prop:
    def __init__(self, prop_id, rule_id, operation, prop_key, prop_value, compare, state,remark1, remark2):
        self.prop_id = prop_id
        self.rule_id = rule_id
        self.operation = operation
        self.prop_key = prop_key
        self.prop_value = prop_value
        self.compare = compare
        self.state = state
        self.remark1 = remark1
        self.remark2 = remark2

    def __str__(self):
        print("PropID:",self.prop_id,'RuleID:', self.rule_id)




class Test:
    def __init__(self):
        self.spark = SparkEnvirnment(app_name='TestPoint', max_cores=2)


    def getRule(self):

        # 获取规则和属性
        rule_result = self.spark.mysql_helper.fetchone("select * from t_POINT_RULE where RULE_ID=1")
        props_result = self.spark.mysql_helper.fetchmany("select * from t_POINT_PROP where RULE_ID=1")


        # 创建规则对象
        rule = Rule(*rule_result,props=props_result)


        # 根据规则表中的source_table 加载相应表
        df = self.spark.load_from_mysql(rule.source_table)


        # 缓存
        l = []

        # 遍历每条属性操作，拼接成sql语句
        for prop in rule.props:
            p = Prop(prop)
            condition = p.prop_key + p.compare + p.prop_value
            # 比较
            if p.operation == 'compare':
                # 筛选条件
                df = df.filter(condition)

            # 计数或求和
            elif p.operation == 'count' or p.operation == 'sum':
                # 计数
                if p.operation == 'count':
                    # 根据主键group by
                    df = df.groupBy('PARIMERY_KEY').count()
                    # 将count重命名为字段名
                    df = df.select('PARIMERY_KEY', df['count'].alias(p.prop_key))
                else:
                    df = df.groupBy('PARIMERY_KEY').sum(p.prop_key)
                    df = df.select('PARIMERY_KEY', df['sum(' + p.prop_key + ')'].alias(p.prop_key))  # sum(ABC)  --> ABC

                df = df.filter(condition)

            # 包含
            elif p.operation == 'in':
                temp = None
                for i in p.operation.split(','):
                    if temp is None:
                        temp = df.filter(condition)
                    else:
                        temp = temp.unionAll(df.filter(condition))


            # 连续型
            elif p.operation == 'continue':
                # df_continue 连续表
                df = self.spark.load_from_mysql('t_POINT_CONTINUE')
                # source_table 和 continue_key 和 update_time 是联合主键
                df = df.filter(df.SOURCE_TABLE == rule.source_table).filter(df.CONTINUE_KEY == p.prop_key)
                df = df.filter(condition)

            else:
                raise Exception('操作类型（%s）无法识别' % p.operation)


        return df


    def test(self):
        df = self.spark.load_from_mysql('LIFE_CYCLE')
        df.show()
        df = df.groupBy('CUSNO').count('SAUM1')
        df.show()









if __name__ == '__main__':
    t = Test()

    t.test()