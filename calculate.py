import pymysql
import re
from setting import db_config
from database_api import MysqlClient


class CreditCalculate():
    def __init__(self,db_config):
        self.client = MysqlClient(db_config)
        self.reduce_di = {1: - 1, 2: - 1, 3: - 3, 4: - 8, 5: - 3, 6: - 10, 7: - 10, 8: - 10, 9: - 12, 10: - 3, 11: - 3,
                     12: - 6, 13: - 12, 14: - 10, 15: - 10, 16: - 12, 17: - 6, 18: - 12}
        self.increase_di = {1: + 3, 2: + 3, 3: + 9, 4: + 24, 5: + 9, 6: + 30, 7: + 30, 8: + 30, 9: + 30, 10: + 9, 11: + 9,
                       12: + 18, 13: + 36, 14: + 30, 15: + 30, 16: + 36, 17: + 18, 18: + 36}
        self.huanping_score = {0:1,1:2,2:1,3:0}
    def create_company(self):
        # 从company_summary数据库获取公司名插入数据库credit_model company_name字段
        query_sql = 'select qichacha_name from company_summary'
        company_tmp = self.client.query(query_sql)
        company_set = {each[0] for each in company_tmp}
        insert_list = [(each,) for each in company_set]
        insert_sql = """insert into credit_model(company_name) values(%s)"""
        self.client.update_many(insert_sql, insert_list)
        print('生成{}条数据'.format(len(insert_list)))
    def get_company(self):
        query_sql = 'select id,company_name from credit_model'
        return self.client.query(query_sql)
    def create_rating(self,year=2018):
        # 从rating表中获取2018年存在信用等级的公司，更新credit_model数据库evaluation_level_2018字段
        id_company = self.get_company()
        query_sql = 'select company,credit_rating from rating where rating_date="{}"'.format(year)
        result = self.client.query(query_sql)
        update_list = []
        for id,company in id_company:
            for each in result:
                if company == each[0]:
                    update_list.append((each[1],id))
                    break   #信用等级数据一般一年一个
        update_sql = 'update credit_model set evaluation_level_{}=%s where id=%s'.format(year)
        self.client.update_many(update_sql,update_list)
        print('生成{}条数据'.format(len(update_list)))
    def create_punish(self,year=2018):
        id_company = self.get_company()
        query_sql = 'select category,xz_name from punishment where cf_date like "{}%"'.format(year) #模糊查询
        result = self.client.query(query_sql)
        update_list = []
        for id,company in id_company:
            record_list = []
            for each in result:
                if company == each[1]:
                    record_list.append(int(each[0]))
            if record_list:
                update_list.append((str(record_list),id))
        print('update_list:',update_list)
        update_sql = 'update credit_model set punish=%s where id=%s'
        self.client.update_many(update_sql,update_list)
        print('生成{}条数据'.format(len(update_list)))
    def create_huanping(self,year=2018):
        id_company = self.get_company()
        query_sql = 'select company,risk_rank from huanping where publicity_time like "{}%"'.format(year)
        result = self.client.query(query_sql)
        update_list = []
        for id,company in id_company:
            record_list = []
            for each in result:
                if company == each[0]:
                    # 无则认为是报告表
                    if not each[1]:
                        record_list.append(0)
                    # 报告书
                    elif int(each[1]) == 1:
                        record_list.append(1)
                    # 报告表
                    elif int(each[1]) == 2:
                        record_list.append(2)
                    elif int(each[1]) == 3:
                        record_list.append(3)
                    elif int(each[1]) == 0:
                        record_list.append(0)
                    else:
                        print('风险项异常',each[1])
            if record_list:
                update_list.append((str(record_list),id))
        print('update_list:',update_list)
        update_sql = 'update credit_model set huanping=%s where id=%s'
        self.client.update_many(update_sql,update_list)
        print('生成{}条数据'.format(len(update_list)))
    def create_check(self,year=2018):
        # 竣工验收
        id_company = self.get_company()
        query_sql = 'select id,company_name from finish_check where submit_date like "{}%"'.format(year)
        result = self.client.query(query_sql)
        update_list = []
        for id,company in id_company:
            record_list = []
            for each in result:
                if company == each[1]:
                    record_list.append(each[0])
            if record_list:
                update_list.append((str(record_list),id))
        print('update_list:',update_list)
        update_sql = 'update credit_model set finish_check=%s where id=%s'
        self.client.update_many(update_sql,update_list)
        print('生成{}条数据'.format(len(update_list)))
    def create_paiwu(self,year=None):
        # 排污许可
        id_company = self.get_company()
        query_sql = 'select id,company_name from finish_check where issue_date like "{}%"'.format(year)
        if not year:
            query_sql = 'select id,company from emission_permit_task'
        result = self.client.query(query_sql)
        update_list = []
        for id,company in id_company:
            record_list = []
            for each in result:
                if company == each[1]:
                    record_list.append(each[0])
            if record_list:
                update_list.append((str(record_list),id))
        print('update_list:',update_list)
        update_sql = 'update credit_model set paiwu=%s where id=%s'
        self.client.update_many(update_sql,update_list)
        print('生成{}条数据'.format(len(update_list)))
    def create_register(self,year=2018):
        # 登记表
        id_company = self.get_company()
        query_sql = 'select id,jsdw from hp_dengji_project_info where notice_date like "{}%"'.format(year)
        result = self.client.query(query_sql)
        update_list = []
        for id,company in id_company:
            record_list = []
            for each in result:
                if company == each[1]:
                    record_list.append(each[0])
            if record_list:
                update_list.append((str(record_list),id))
        print('update_list:',update_list)
        update_sql = 'update credit_model set huanping_register=%s where id=%s'
        self.client.update_many(update_sql,update_list)
        print('生成{}条数据'.format(len(update_list)))

    def calculate_level(self,score,type='+'):
        if type == '+':
            if score <= 10:
                return '绿色企业'
            elif score <= 20:
                return '蓝色企业'
            elif score <= 30:
                return '黄色企业'
            elif score <= 40:
                return '红色企业'
            else:
                return '黑色企业'
        else:
            if score >= 11:
                return '绿色企业'
            elif score >= 6:
                return '蓝色企业'
            elif score >= 3:
                return '黄色企业'
            elif score >= 1:
                return '红色企业'
            else:
                return '黑色企业'

    def calculate_public(self):
        # 利用2018年信用等级结合修正项计算
        sql = 'select * from credit_model where evaluation_level_2018 is not null'
        content = self.client.query(sql)
        update_list = []
        for each in content:
            score = 0
            id = each[0]
            company_name = each[1]
            credit = each[3]
            if '绿色' in credit:
                score = 5
            elif '蓝色' in credit:
                score = 15
            elif '黄色' in credit:
                score = 25
            elif '红色' in credit:
                score = 35
            elif '黑色' in credit:
                score = 40
            else:
                print("信用评级异常")
            # 行政处罚
            punish = each[4]
            if punish:
                punish_list = eval(punish)
                for punish_unit in punish_list:
                    score += self.increase_di.get(int(punish_unit))
            # 环评
            huanping = each[5]
            if huanping:
                huanping_list = eval(huanping)
                for huanping_unit in huanping_list:
                    score += self.huanping_score.get(int(huanping_unit))
            else:
                score += 1
            # 环评登记
            huanping_register = each[6]
            if huanping_register:
                huanping_regist_list = eval(huanping_register)
                for huanping_register_unit in huanping_regist_list:
                    score += 0
            # 竣工验收
            finish_check = each[7]
            if finish_check:
                finish_check_list = eval(finish_check)
                for finish_check_unit in finish_check_list:
                    score += 0
            # 排污许可
            paiwu = each[8]
            if not paiwu:
                score += 0
            rank = self.calculate_level(score,'+')
            update_list.append((score,rank,id))
        print('update_list:',update_list)
        update_sql = 'update credit_model set score=%s,credit_rating=%s where id=%s'
        self.client.update_many(update_sql,update_list)
        print('生成{}条数据'.format(len(update_list)))

    def calculate_loss(self):
        # 利用行政处罚记录对不存在信用等级的公司分类
        sql = 'select * from credit_model where evaluation_level_2018 is null'
        content = self.client.query(sql)
        update_list = []
        for each in content:
            score = 9
            score +=3  # 3年无不良记录
            id = each[0]
            company_name = each[1]
            # 行政处罚
            punish = each[4]
            if punish:
                punish_list = eval(punish)
                for punish_unit in punish_list:
                    score += self.reduce_di.get(int(punish_unit))
            rank = self.calculate_level(score,'-')
            update_list.append((score,rank,id))
        print('update_list:',update_list)
        update_sql = 'update credit_model set score=%s,credit_rating=%s where id=%s'
        self.client.update_many(update_sql,update_list)
        print('生成{}条数据'.format(len(update_list)))


def update_summary_rating():
    # 计算结果更新至company_summary表
    client = MysqlClient(db_config)
    sql = 'select credit_rating,company_name from credit_model'
    content = client.query(sql)
    result = client.query('select id,qichacha_name from company_summary')
    update_list = []
    for credit,company in content:
        for id,qichacha in result:
            if company == qichacha:
                update_list.append((credit,id))
    print('update_list:',update_list)
    update_sql = 'update company_summary set credit_rating=%s where id=%s'
    client.update_many(update_sql,update_list)





if __name__ == '__main__':
    # cc = CreditCalculate(db_config)
    # cc.calculate_loss()
    update_summary_rating()












