'''
项目任务调度总成

1.日AUM     每日
2.生命周期   每季度
3.AUM总和    每月？？？
4.DEBT负债总和  每月？？？
5.客户价值    每半年

'''

from product.analysis import DataAnalysis
import datetime
from apscheduler.schedulers.background import BlockingScheduler
try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os
    sys.path.append(os.path.abspath('../'))
    from product.mysql_helper import MySQLHelper

class CMMSTask:
    def __init__(self):
        self.mysql_helper = MySQLHelper('core', host='10.9.29.212')

    def daily_task(self):
        def func():
            da = DataAnalysis()
            day = datetime.datetime.now().strftime('%Y-%m-%d')
            # 活期
            da.init_balance(day, 1)
            # 定期
            da.init_balance(day, 2)

        scheduler = BlockingScheduler()
        scheduler.add_job(func,'cron',day='*',hour='1')

        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            # TODO 执行错误的处理方案
            scheduler.shutdown()




    def month_task(self):
        pass



    def seasonly_task(self):
        pass
