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


    def execute(self,sql,args=()):
        self.cur.execute(sql,args)
        self.cnn.commit()

    def executemany(self,sql,args=()):
        self.cur.executemany(sql,args)
        self.cnn.commit()

    def fetchone(self,sql,args=()):
        self.cur.execute(sql, args)
        return self.cur.fetchone()

    def fetchmany(self,sql,args=(),size=None):
        self.cur.execute(sql,args)
        return self.cur.fetchmany(size)

    def fetchall(self,sql,args=()):
        self.cur.execute(sql, args)
        return self.cur.fetchall()



if __name__ == '__main__':
    '''
    导入银联商户码表
    '''
    mysql_helper = MySQLHelper()
    file = '''0742 兽医服务
0743 葡萄酒生产商
0744 香槟生产商
0763 农业合作
0780 景观美化和园艺服务
1520 一般承包商—住宅和商业楼
1711 供暖,管道,空调承包商
1731 电气承包商
1740 砖,石,瓦,石膏和绝缘工程承包商
1750 木工工程承包商
1761 屋顶,屋围,金属片(铁皮)安装工程承包商
1771 混凝土工程承包商
1799 未列入其它代码的专项贸易承包商
2741 各种出版和印刷服务
2791 排版,刻版及相关服务
2842 专业清洁,抛光和卫生服务
4011 铁路运输
4111 本地和市郊通勤旅客运输(包括轮渡)
4112 铁路客运
4119 救护车服务
4121 出租车和豪华轿车服务
4131 公共汽车
4214 长短途机动车与卡车货运,搬运公司,仓储公司,本地专运公司
4215 快递服务(空运,地面运输或海运)
4225 公共仓储服务-销售,维修和修复服务
5933 当铺
5935 海上船只遇难救助场
5937 古玩复制店
5940 自行车商店—销售和服务
5941 体育用品店
5942 书店
5943 文具,办公,学校用品商店
5944 珠宝,手表,钟表和银器商店
5945 玩具游戏店
5946 照相器材店
5947 礼品,卡片,装饰品,纪念品商店
5948 箱包,皮具店
5949 缝纫,刺绣,织物和布料商店
5950 玻璃器具和水晶饰品商店
5960 直销-保险服务
5962 电话销售—旅行相关的服务
5963 送货上门销售
5964 直销—目录邮购商
5965 直销—目录邮购与零售兼营的商户
5966 直销—呼出型电话行销商
5967 直销—接入型电话行销商
5968 直销—长期定购或会员制商户
5969 未列入其它代码的直销业务和直销商
5970 工艺美术商店
5971 艺术商和画廊
5972 邮票和纪念币商店
5973 宗教品商店
5975 助听器—销售,服务和用品
5976 假肢店
5977 化妆品商店
5978 打字机商店—销售,服务和出租
5983 燃料经销商—燃油,木材,煤炭和液化石油
5992 花店
5993 雪茄店
5994 报亭,报摊
5995 宠物商店,宠物食品及用品
5996 游泳池—销售,供应和服务
5997 电动剃刀商店—销售和服务
5998 帐篷和遮阳篷商店
5999 其它专营零售店
6010 金融机构—人工现金支付
6011 金融机构—自动现金支付
6012 金融机构—购买商品和服务
6051 非金融机构—外币兑换,非电子转帐的汇票,临时支付凭证和旅行支票
6211 证券公司—经纪人和经销商
6300 保险销售,保险业和保险金
6760 储蓄
7011 住宿服务(旅馆,酒店,汽车旅馆,度假村)
7012 分时使用的别墅或度假用房
7032 运动和娱乐露营地
7033 活动房车场及露营场所
7210 洗衣服务
7211 洗熨服务—家庭和商业
7216 干洗店
7217 室内清洁服务(地毯,沙发,家具表面)
7221 摄影工作室
7230 美容理发店
7251 修鞋店,擦鞋店,帽子清洗店
7261 丧仪殡葬服务
7273 婚姻介绍及陪同服务
7276 交税准备服务
7277 咨询服务—债务,婚姻和个人私事
7278 购物服务及会所
7295 家政服务
7296 出租衣物—服装,制服和正式场合服装
7297 按摩店
7298 健身及美容室
7299 未列入其它代码的其他个人服务
7311 广告服务
7321 消费者信用报告机构
7322 债务催收机构
7333 商业摄影,工艺,绘图服务
7338 快速复印,复制及绘图服务
7339 速记和秘书类服务
7342 灭虫(蝇等)及消毒服务
7349 清洁,保养,门卫服务
7361 职业中介,临时帮佣服务
7372 计算机编程,数据处理和系统集成设计服务
7375 信息检索服务
7379 未列入其它代码的计算机维护和修理服务
7392 管理,咨询和公共关系服务
7393 侦探,保安,安全服务(包括防弹车和警犬)
7394 设备,工具,家俱和电器出租
7395 照相洗印服务
7399 未列入其它代码的商业服务
7512 汽车出租
7513 卡车及拖车出租
7519 房车和娱乐车辆出租
7523 停车场及车库
7531 车体维修店
7534 轮胎翻新,维修店
7535 汽车喷漆店
7538 汽车服务商店(非经销商)
7542 洗车
7549 拖车服务
7622 电器维修
7623 空调及冷藏设备维修店
7629 电气设备及小家电维修店
7631 手表,钟表和首饰维修店
7692 焊接维修服务
7699 各类维修店及相关服务
7829 电影和录像带制片,发行
7832 电影院
7841 出租录像带服务
7922 戏剧制片(不含电影),演出,票务
7929 未列入其它代码的乐队,管弦乐队和各类演艺人员
7932 台球,撞球场所
7933 保龄球馆
7941 商业体育场馆,职业体育俱乐部,运动场和体育推广公司
7991 旅游,展览
7992 公共高尔夫球场
7993 电子游戏供给
7994 大型游戏机和游戏场所
7995 博彩业(包括彩票等)
7996 游乐园,马戏团,嘉年华等
7997 成员俱乐部(体育,娱乐,运动),乡村俱乐部和私人高尔夫球场
7998 水族馆,海洋馆和海豚馆
7999 未列入其它代码的其他娱乐服务
8011 未列入其它代码的医生和医师
8021 牙科医生,整牙医生
8031 正骨医生
8041 按摩医生
8042 验光配镜师,眼科医生
8043 光学仪器商,光学产品和眼镜商
8049 手足病医生
8050 护理和照料服务
8062 医院
8071 医学和牙科实验室
8099 未列入其它代码的医疗保健服务
8111 法律服务和律师事务所
8211 小学和中校
8220 大学,学院,专科和职业学院
8241 函授学校
8244 商业和文秘学校
8249 贸易和职业学校
8299 未列入其它代码的学校与教育服务
8351 儿童保育服务
8398 慈善和社会公益服务组织
8641 公民社团和共济会
8651 政治组织
8661 宗教组织
8675 汽车协会
8699 未列入其它代码的会员组织
8734 测试实验室(非医学)
8911 建筑,工程和测量服务
8912 装修,装潢,园艺
8931 会计,审计,财务服务
8999 未列入其它代码的其他专业服务
9211 法庭费用,包括赡养费和子女抚养费
9222 罚款
9223 保释金
9311 纳税
9399 未列入其它代码的政府服务
9400 使领馆收费
9404 邮政服务—仅限政府'''
    list = file.split('\n')
    t_list = []

    for i in list:
        si = i.split(' ')
        t_list.append((si[0], si[1]))

    for t in t_list:
        print(t)

    sql = "replace into t_CMMS_CREDIT_MERCODE(CODE,CATEGORY) values(%s,%s)"

    mysql_helper.executemany(sql, t_list)