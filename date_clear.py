import re
from setting import db_config
from database_api import MysqlClient
import time



class DateClear():
    def __init__(self,db_config,table_name,field_name):
        # company_field属性代表表中表示公司名的字段，行政处罚记录表用xz_name，其他一般用company
        self.client = MysqlClient(db_config)
        self.table_name = table_name
        self.field_name = field_name
        self.del_style = 0 #是否备份删除值 0：不保留  1：备份到 _del表格

    def update_date(self):
        # 根据触发内容更新处罚类别
        query_sql = "select id,{} from {}".format(self.field_name,self.table_name)
        contents = self.client.query(query_sql)
        update_list = []
        for id_date in contents:
            id = id_date[0]
            cf_date = id_date[1]
            if not cf_date:
                pass
            elif re.findall('(\d{4})[-/\.年](\d{1,2})[-/\.月](\d{1,2})',cf_date,re.S):
                tmp = re.findall('(\d{4})[-/\.年](\d{1,2})[-/\.月](\d{1,2})',cf_date,re.S)[0]
                date_li = [each if len(each)%2==0 else '0'+each for each in tmp]
                date_new = '-'.join(date_li)
                update_list.append((date_new,id))
            elif re.findall('^(20\d{6})$',cf_date,re.S):
                tmp_date = re.findall('^(20\d{6})$',cf_date,re.S)[0]
                date_new = tmp_date[:4]+'-'+tmp_date[4:6]+'-'+tmp_date[6:]
                update_list.append((date_new, id))
            else:
                print(cf_date)
        print('update_list:',update_list)
        update_sql = "update {} set {}=%s where id=%s".format(self.table_name,self.field_name)
        self.client.update_many(update_sql,update_list)
        print('更新{}条数据'.format(len(update_list)))
    def update_timestamp(self):
        # 根据触发内容更新处罚类别
        query_sql = "select id,{} from {}".format(self.field_name,self.table_name)
        contents = self.client.query(query_sql)
        update_list = []
        for id_date in contents:
            id = id_date[0]
            cf_date = id_date[1]
            if len(cf_date) == 10:
                timeArray = time.localtime(int(cf_date))
                date_new = time.strftime("%Y-%m-%d", timeArray) # %H:%M:%S
                update_list.append((date_new, id))
        print('update_list:',update_list)
        update_sql = "update {} set {}=%s where id=%s".format(self.table_name,self.field_name)
        self.client.update_many(update_sql,update_list)
        print('更新{}条数据'.format(len(update_list)))



if __name__ == '__main__':
    dc = DateClear(db_config,'finish_check','submit_date')
    dc.update_timestamp()