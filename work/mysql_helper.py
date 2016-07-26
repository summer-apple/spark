import mysql.connector



class MySQLHelper:
    def __init__(self,database,host='localhost',port='3306',user='root',password='',charset='utf8'):
        config = {'host': host,
                  'user': user,
                  'password': password,
                  'port': port,
                  'database': database,
                  'charset': charset
                  }

        try:
            self.cnn = mysql.connector.connect(**config)
            self.cur = self.cnn.cursor()

        except mysql.connector.Error as e:
            print('connect fails!{}'.format(e))


    def execute(self,sql,args):
        self.cur.execute(sql,args)
        self.cnn.commit()

    def executemany(self,sql,args):
        self.cur.executemany(sql,args)
        self.cnn.commit()

    def fetchone(self,sql,args):
        self.cur.execute(sql, args)
        return self.cur.fetchone()

    def fetchmany(self,sql,args,size=None):
        self.cur.execute(sql,args)
        return self.cur.fetchmany(size)

    def fetchall(self,sql,args):
        self.cur.execute(sql, args)
        return self.cur.fetchall()



