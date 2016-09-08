__author__ = 'jjzhu'

import pymysql
# from PySparkUsage.util.logger import Logger


class MysqlConnection:
    """
    连接mysql数据库
    """
    # 这里要注意的是charset必须指定，不然数据插入和读取会出现乱码
    def __init__(self, db, host=u'localhost', port=3306, user=u'root', passwd=u'', charset=u'utf8'):

        self.connection = pymysql.connect(db=db, host=host, port=port, user=user, passwd=passwd, charset=charset)
        self.cur = self.connection.cursor()
        # self.logger = Logger()

    # 执行单条查询语句
    def execute_single(self, sql, args=None):
        # self.logger.info('execute query sql:%s, args:%s' % (str(sql), str(args)))
        self.cur.execute(sql, args)
        self.connection.commit()

    def call_proc(self, proc_name, args=()):
        print(args)
        self.cur.callproc(proc_name, args)

    # 批量执行查询语句
    def execute_many(self, sql, args):
        self.cur.executemany(sql, args)
        self.connection.commit()

    # 执行查询语句并返回查询结果
    def select_query(self, sql):
        result = []
        self.cur.execute(sql)
        for item in self.cur:
            result.append(item)
        return result

    def exist(self, sql):
        return self.cur.execute(sql)

    # 关闭mysql连接
    def close(self):
        self.cur.close()
        self.connection.close()

if __name__ == '__main__':
    conn = MysqlConnection(db='core', host='10.9.29.212', passwd='')
    conn.execute_single('insert into core.datetest(time) value ("2016-01-01")')
    print('successful')