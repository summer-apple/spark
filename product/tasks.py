'''
项目任务调度总成

1.日AUM     每日
2.生命周期   每季度
3.AUM总和    每月？？？
4.DEBT负债总和  每月？？？
5.客户价值    每半年

'''

from product.jj_analysis import DataAnalysis
from product.band_card import DataHandler
import datetime
from apscheduler.schedulers.background import BlockingScheduler
import logging
import logging.config



try:
    from mysql_helper import MySQLHelper
except ImportError:
    import sys, os
    sys.path.append(os.path.abspath('../'))
    from product.mysql_helper import MySQLHelper

class CMMSTask:
    def __init__(self):
        self.mysql_helper = MySQLHelper('core', host='10.9.29.212')
        logging.config.fileConfig('./conf/logging.conf')
        self.logger = logging.getLogger('simpleLogger')
        self.da = DataAnalysis()
        self.dh = DataHandler()

    def daily_task(self):
        def func():
            day = datetime.datetime.now().strftime('%Y-%m-%d')
            # 活期
            self.da.init_balance(day, 1)
            self.logger.info(day, '活期每日余额计算完成')
            # 定期
            self.da.init_balance(day, 2)
            self.logger.info(day, '定期每日余额计算完成')
            # 理财
            self.da.init_balance(day, 3)
            self.logger.info(day, '理财每日余额计算完成')

        scheduler = BlockingScheduler()
        scheduler.add_job(func,'cron',day='*',hour='1') # 每天凌晨1点运行

        try:
            scheduler.start()
        except Exception as e:
            # TODO 执行错误的处理方案
            self.logger.error('每日AUM计算出错:',e)
            scheduler.shutdown()




    def month_task(self):

        def func():
            self.dh.aum_total()
            self.dh.debt_total()


        scheduler = BlockingScheduler()
        scheduler.add_job(func, 'cron', month='*/1', day='1', hour='5') # 每月一号五点运行




    def seasonly_task(self):
        def func():
            # 每个月计算前一个月的数据
            month = datetime.datetime.now().month - 1
            year = datetime.datetime.now().year
            if month == 0:
                month = 12
                year = year-1

            season = month/3

            # 计算生命周期
            self.dh.run_life_cycle(year,season)

        scheduler = BlockingScheduler()
        scheduler.add_job(func, 'cron', month='1,4,7,10', day='2', hour='2')


    def half_year_task(self):
        def func():
            month = datetime.datetime.now().month - 1
            year = datetime.datetime.now().year
            if month == 0:
                month = 12
                year = year - 1

            half_year = month/6

            self.dh.customer_value(year,half_year)

        scheduler = BlockingScheduler()
        scheduler.add_job(func, 'cron', month='7,12', day='2', hour='5') # 7月12月2号五点计算客户价值