import os, sys, re
import logging
import logging.config


# 用于解压农信导出的tar.Z数据库文件
# 代码很烂

class Untarz:
    def __init__(self):
        self.input_path = '/media/summer/1TB/pj_data'
        self.output_path = '/media/summer/1TB/pj_data_untarz'
        logging.config.fileConfig('./conf/logging.conf')
        self.logger = logging.getLogger('simpleLogger')


    def untarz(self):
        '''
        --CORE
            --20160403
                -xxx1.tar.Z
            --20160405
                -xxx2.tar.Z
        :return:
        '''


        # 文件夹下所有文件及文件夹
        # [CORE,CARD,FMD,...]
        center_file_list = os.listdir(self.input_path)

        temp = []

        # 大类均以字母开头
        r1 = r'^[A-Z]'
        r2 = r'\d{8}'
        r3 = r'.tar.Z$'


        #第一层[CAED,CRRD,...]
        for center in center_file_list:
            # f: 大类名CARD
            # 如果是系统大类文件夹
            # TODO 还是大类路径（'/media/summer/1TB/pj_data/BTOP'），需要进入目录取到日期目录列表
            if re.match(r1,center) is None:
                continue
            center_input_path = os.path.join(self.input_path,center) # /media/summer/1TB/pj_data/BTOP
            center_output_path = os.path.join(self.output_path, center) # /media/summer/1TB/pj_data_untarz/BTOP
            logging.debug('center_input_path:' + center_input_path)
            logging.debug('center_output_path:' + center_output_path)






            # 第二层[20160304,20160405,...]
            daily_file_list = os.listdir(center_input_path)

            for daily in daily_file_list:
                if re.match(r2, daily) is None:
                    continue





                # [xxx.tar.Z,...]一般目录里只有一个压缩文件
                tarz_file_list = os.listdir(os.path.join(center_input_path, daily))
                for tarz in tarz_file_list:
                    if not tarz.endswith('.tar.Z'):
                        continue
                        # 截去后缀


                    tarz_file_path = os.path.join(os.path.join(center_input_path, daily), tarz) # /media/summer/1TB/pj_data/ZZQZ/20160425/ZZQZ_909000_20160425_ADD.tar.Z
                    logging.debug('tarz_file_path:' + tarz_file_path)

                    output_path = os.path.join(center_output_path, daily) # /media/summer/1TB/pj_data_uptarz/ZZQZ/20160425
                    logging.debug('output_path:' + output_path)

                    # 判断解压目标目录是否存在
                    if not os.path.exists(output_path):
                        mkdir_cmd = 'mkdir ' + output_path # 建目录
                        mkdirs_cmd = 'mkdir -p ' + output_path # 建多层目录

                        return_code = os.system(mkdir_cmd) # 0：OK ，256：faild
                        if return_code == 0:
                            logging.info(mkdir_cmd)
                        else:
                            os.system(mkdirs_cmd)
                            logging.info(mkdirs_cmd)


                    # 解压
                    untarz_cmd = 'tar -xf ' + tarz_file_path + ' -C ' + output_path
                    os.system(untarz_cmd)
                    logging.info(untarz_cmd)











if __name__ == '__main__':
    u = Untarz()
    u.untarz()



