import re
from setting import db_config
from database_api import MysqlClient


def test():
    client = MysqlClient(db_config)
    sql = 'select cf_number from punishment'
    rs = client.query(sql)
    for each in rs:
        print(each)

class PunishClear():
    def __init__(self,db_config,field):
        # company_field属性代表表中表示公司名的字段，行政处罚记录表用xz_name，其他一般用company
        self.company_field = field
        self.company_index = 13   #公司索引
        self.client = MysqlClient(db_config)
        self.table_name = 'punishment'
        self.del_style = 0 #是否备份删除值 0：不保留  1：备份到 _del表格

    def get_companys(self):
        # 返回公司列表，由id,company字段
        query_sql = "select id,{} from {}".format(self.company_field,self.table_name)
        contents = self.client.query(query_sql)
        return contents
    def get_all(self):
        # 返回公司列表，由id,company字段
        query_sql = "select * from {}".format(self.table_name)
        contents = self.client.query(query_sql)
        return contents

    def update_empty_company(self):
        # 清除公司名中的换行符、空格、制表符等空格形式
        for id_company in self.get_companys():
            id = id_company[0]
            company = id_company[1]
            if not company:
                continue
            if re.findall(r"\s", company, re.S):
                company_new = re.sub("\s", "", company)
                print('update:', company)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)

    def update_cf_category(self):
        # 根据处罚内容更新处罚类别
        query_sql = "select id,cf_result from punishment"
        contents = self.client.query(query_sql)
        update_list = []
        for id_cf in contents:
            id = id_cf[0]
            cf_result = id_cf[1]
            #不存在处罚，默认为罚款
            if not cf_result:
                category = 3
            # 12分 category类别
            elif re.findall('发生[重|特大]环境污染事故',cf_result,re.S):
                category = 18
            elif re.findall('刑事责任',cf_result,re.S):
                category = 16
            elif re.findall('责令停业关闭',cf_result,re.S):
                category = 9
            elif re.findall('吊销许可证',cf_result,re.S):
                category = 13
            # 10分
            elif re.findall('(?:行政拘留|强制执行)',cf_result,re.S):
                category = 15
            elif re.findall('(?:停产|停止生产|恢复原状|停止.*?项目)',cf_result,re.S):
                category = 6
            # 8分
            elif re.findall('停止建设',cf_result,re.S):
                category = 4
            # 6分
            elif re.findall('按日连续处罚',cf_result,re.S):
                category = 17
            elif re.findall('暂扣许可证',cf_result,re.S):
                category = 12
            # 3分
            elif re.findall('责令限制生产',cf_result,re.S):
                category = 5
            elif re.findall('(?:查封|扣押)',cf_result,re.S):
                category = 10
            elif re.findall('没收(?:违法所得|非法财物)',cf_result,re.S):
                category = 11
            elif re.findall('(?:罚款|处罚.*?元|缴至.*?银行和账号|处以.*?万元)',cf_result,re.S) or re.findall('\d+万?元',cf_result,re.S) or re.findall('^\d+(\.0)?$',cf_result,re.S) :
                category = 3
            # 1分
            elif re.findall('(?:停止|改正).*?行为',cf_result,re.S):
                category = 2
            elif re.findall('警告',cf_result,re.S):
                category = 1
            #无法判断的默认为罚款
            else:
                category = 3
            update_list.append((category,id))
        print('update_list:',update_list)
        update_sql = "update punishment set category=%s where id=%s"
        self.client.update_many(update_sql,update_list)
        print('生成{}条数据'.format(len(update_list)))

    def update_cf_date(self):
        # 根据触发内容更新处罚类别
        query_sql = "select id,cf_date from punishment"
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
                pass
        # print('update_list:',update_list)
        update_sql = "update punishment set cf_date=%s where id=%s"
        self.client.update_many(update_sql,update_list)
        print('更新{}条数据'.format(len(update_list)))

    def update_extra_information(self):
        '''
        清除公司名中多余的信息，‘2016-49号-岳阳凯盛化工有限公司’,'8.28-东莞市尚濠灯饰有限公司','80山西同煤电力环保科技有限公司
        "宜昌市康复医院  （广州市花都区道路交通基础设施建设管理中心）   建设单位：舟山市临城新区开发建设有限公司
        '''
        for id_company in self.get_companys():
            id = id_company[0]
            # print("=========={}=========".format(id))
            company = id_company[1]
            if not company:
                continue
            if '〔' in company or '【' in company or '】' in company or '〕' in company or '﹝' in company or '﹞' in company:
                company_new = company.replace('【','[').replace('】',']').replace('〕',']').replace('〔','[').replace('﹝','[').replace('﹞',']')
                print('update:', company)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            if re.findall('（\d+）',company,re.S):
                sub_tmp = re.findall('（\d+）',company,re.S)[0]
                company_new = company.replace(sub_tmp,sub_tmp.replace('（','[').replace('）',']'))
                print('update:', company)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            if re.findall('(.*号)（\w+）',company,re.S):
                company_new = re.findall('(.*号)（\w+）',company,re.S)[0]
                print('update:', company)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            if not re.findall(r'^(?:[\u4E00-\u9FA5]|（).*?环.*?[罚责改字][\u4E00-\u9FA5]{0,5}\[\d+\]第?\w{0,5}\d+号$',company,re.S):
                print(company,re.findall('^(?:[\u4E00-\u9FA5]|（).*?环.*?[罚责改字][\u4E00-\u9FA5]{0,5}\[\d+\]第?\w{0,5}\d+号$',company,re.S))

    def update_bract(self):
        # 把英文括号替换为中文括号
        for id_company in self.get_companys():
            id = id_company[0]
            # print("=========={}=========".format(id))
            company = id_company[1]
            if not company:
                continue
            if re.findall(r"[\(\)]", company, re.S):
                company_new = company.replace("(","（").replace(")","）")
                print('update:', company)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)

    def del_empty(self):
        # 清除公司名为空的数据
        content_list = self.get_companys()
        if self.del_style == 1:
            content_list = self.get_all()
        for content in content_list:
            id = content[0]
            # print("=========={}=========".format(id))
            company = content[1]
            if self.del_style == 1:
                company = content[self.company_index]
            if not company:
                if self.del_style == 1:
                    print("copy:", company)
                    insert_sql = 'insert into table_name_del values ({})'.format(','.join(['%s'] * len(content)))
                    insert_sql = insert_sql.replace('table_name', self.table_name)
                    self.client.update_many(insert_sql, [content])
                print("delete:", id)
                delete_sql = "delete from {} where id={}".format(self.table_name,id)
                self.client.delete(delete_sql)

    def del_normal_word(self,removes=['环']):
        # 删除不存在特殊文字或符号的公司
        content_list = self.get_companys()
        if self.del_style == 1:
            content_list = self.get_all()
        for content in content_list:
            id = content[0]
            company = content[1]
            if self.del_style == 1:
                company = content[self.company_index]
            for remove in removes:
                if remove not in company:
                    if self.del_style == 1:
                        print("copy:", company)
                        insert_sql = 'insert into table_name_del values ({})'.format(','.join(['%s'] * len(content)))
                        insert_sql = insert_sql.replace('table_name', self.table_name)
                        self.client.update_many(insert_sql, [content])
                    print("delete:", company)
                    delete_sql = "delete from {} where id={}".format(self.table_name,id)
                    self.client.delete(delete_sql)
                    break

    def del_repeat(self):
        # 根据处罚文号和公司名去除重复数据
        sql = "select cf_number,xz_name from punishment"
        content_list = list(self.client.query(sql))
        content_set = set(content_list)
        del_list = []
        for each in content_set:
            repeat_sql = 'select id,cf_result from punishment where cf_number="{}"and xz_name="{}"'.format(each[0],each[1])
            if content_list.count(each) > 1:
                tmp_content = self.client.query(repeat_sql)
                compare_list = [(each[0],len(each[1]) if each[1] else 0) for each in tmp_content]
                sort_list = sorted(compare_list, key=lambda x:x[1],reverse=True)
                new_list = sort_list[1:]
                del_tmp = [(each[0],) for each in new_list]
                del_list.extend(del_tmp)
        del_sql = 'delete from punishment where id=%s'
        print('del_list:',del_list)
        self.client.update_many(del_sql,del_list)
        print('去重成功！')





    def run(self):
        #主函数，调用所需其他方法
        print("start function:",'update_empty_company' )
        self.update_empty_company()
        # print("start function:", 'update_bract')
        # self.update_bract()
        # print("start function:", 'update_extra_information')
        # self.update_extra_information()
        # print("start function:", 'update_other')
        # self.update_other()
        # print("start function:", 'del_empty')
        # self.del_empty()
        # print("start function:", 'del_specific_end')
        # self.del_specific_end()
        # print("start function:", 'del_unnormal_end')
        # self.del_unnormal_end()
        # print("start function:", 'del_unnormal_length')
        # self.del_unnormal_length()
        # print("start function:", 'del_unnormal_word')
        # self.del_unnormal_word()
        # print("start function:", 'del_other')
        # self.del_other()

if __name__ == '__main__':
    obj = PunishClear(db_config,'cf_date')
    obj.update_cf_category()
    # rs = obj.get_companys()
    # for each in rs:
    #     print(each)
    # re.findall(r'[\u4E00-\u9FA5]+环.*?罚[\u4E00-\u9FA5]+\[\d+\]第?\d+号',company,re.S)
    # rs = re.findall(r'^[\u4E00-\u9FA5]+环.*?罚[\u4E00-\u9FA5]{0,5}\[\d+\]第?\d+号$', '沾环罚[2017]09号', re.S)
    # print(rs)

