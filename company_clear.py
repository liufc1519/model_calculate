import pymysql
import re
from setting import db_config
from database_api import MysqlClient
# import jieba


class CompanyClear():
    '''
    该类的目的并不是将无效公司名处理成有效公司，而是提取有效公司名。有效是指名称结尾是常用词汇，名称字符长度适中
    '''
    def __init__(self,db_config,table_name):
        # company_field属性代表表中表示公司名的字段，行政处罚记录表用xz_name，其他一般用company
        self.company_field = 'jsdw'
        self.company_index = 3   #公司索引
        self.client = MysqlClient(db_config)
        self.table_name = table_name
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

    def count(self):
        # 获取公司总数
        query_sql = "select * from {}".format(self.table_name)
        numbs = len(self.client.query(query_sql))
        return numbs

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
            if re.findall(r"号-.*", company, re.S):
                company_new = re.findall(r"号-(.*)", company, re.S)[0]
                print('update:', company)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall(r"\d+\.\d+-(.*)", company, re.S):
                company_new = re.findall(r"\d+\.\d+-(.*)", company, re.S)[0]
                print('update:', company)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall(r"^\d+[\u4E00-\u9FA5]+$", company, re.S):
                company_new = re.findall(r"^\d+([\u4E00-\u9FA5]+)$", company, re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall(r"^（(.*)）$", company, re.S):
                company_new = re.findall(r"^（(.*)）$", company, re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall(r"^\W+(\w{4,}.*)", company, re.S):
                company_new = re.findall(r"^\W+(\w{4,}.*)", company, re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall(r"(.*?)[。\.：:\-、；;?]+$", company, re.S):
                company_new = re.findall(r"(.*?)[。\.：:\-、；;?]+$", company, re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall(r"^[\u4E00-\u9FA5]{3,5}[：:][\u4E00-\u9FA5]+$", company, re.S):
                company_new = re.findall(r"^[\u4E00-\u9FA5]{3,5}[：:]([\u4E00-\u9FA5]+)$", company, re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall(r"(.*[\u4E00-\u9FA5])[^\u4E00-\u9FA5]其他.*", company, re.S):
                company_new = re.findall(r"(.*[\u4E00-\u9FA5])[^\u4E00-\u9FA5]其他.*", company, re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall(r"^[^\u4E00-\u9FA5]([\u4E00-\u9FA5]{4,}$)", company, re.S):
                company_new = re.findall(r"^[^\u4E00-\u9FA5]([\u4E00-\u9FA5]{4,}$)", company, re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall('(.*)（.*\d+年.*?）$',company,re.S):
                '''平凉泾瑞环保科技有限公司（合同签订日期为2017年1月）'''
                company_new = re.findall('(.*)（.*\d+年.*?）$',company,re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall('(.*)（.*?第\d+号）$',company,re.S):
                '''江苏润环环境科技有限公司（国环评证甲字第1907号）'''
                company_new = re.findall('(.*)（.*?第\d+号）$',company,re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            elif re.findall('^[\u4E00-\u9FA5]{2,4}（(.*?(?:公司|厂|医院|中心))）$',company,re.S):
                company_new = re.findall('^[\u4E00-\u9FA5]{2,4}（(.*?(?:公司|厂|医院|中心))）$',company,re.S)[0]
                print('update:', company)
                print("new_company:",company_new)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)

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

    def update_other(self):
        # 整改重复公司数据，如德格县自来水公司德格县自来水公司 中盐重庆长寿盐化有限公司（污染次数0次）
        for id_company in self.get_companys():
            id = id_company[0]
            company = id_company[1]
            if not company:
                continue
            length = len(company)
            if length % 2 == 0 and (company[:int(length/2)] == company[int(length/2):]):
                company_new = company[:int(length/2)]
                print('update:', company)
                update_sql = "update {} set {}='{}' where id={}".format(self.table_name,self.company_field,company_new,id)
                self.client.update(update_sql)
            if re.findall('（污染次数0次）',company):
                company_new = company.replace('（污染次数0次）','')
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

    def del_unnormal_end(self):
        # 删除不常用公司名
        content_list = self.get_companys()
        if self.del_style == 1:
            content_list = self.get_all()
        for each in content_list:
            id = each[0]
            company = each[1]
            if self.del_style == 1:
                company = each[self.company_index]
            company_legal = re.findall(r".*(?:公司|中心|医院|餐厅|小吃店|店|政府|委员会|管理局|公室|厂|工厂|诊所|门诊部|管理处|制品厂\
                  |设局|供电局|水务局|运输局|管理所|工部|办事处|局|快餐店|学院|饭庄|合作社|加油站|收购点|中学|大学|火锅店|研究所|油厂|分店|\
                  学校|指挥部|联合社|面馆|卫生院|小学|菜馆|酒家|卫生所|饭店|水厂|服务区|木业|酒楼|研究院|饮店|餐馆|小吃部|服务部|政局|水利局|分局|家具厂|\
                  饮食店|馆|金厂|教育局|酒店|材料厂|食府|保健院|石场|啡厅|机械厂|交通局|修理厂|洗衣店|食品店|饭馆|体育局|营部|委会|服务站|\
                  电厂|分厂|养殖场|粉店|印刷厂|保护局|包子铺|路局|电子厂|城|饺子馆|部|商店|社|食品厂|酒吧|坊|加工点|卫生局|事业部|公安局|干洗店|\
                  模具厂|大队|舞厅|宾馆|艺品厂|园林局|处理厂|石材厂|室|农场|砖厂|矿|处|纸厂|维修部|目部|维修厂|建材厂|料理店|测站|设备厂|\
                  肉店|管理站|工场|制造厂|俱乐部|花园|小食店|配件厂|烧烤店|院|修理部|执法局|火锅城|集团|总站|支队|所|站|煤矿|咖啡店|美容店|塑料厂|\
                  法院|市场|厨房|园|林业局|咖啡馆|塑胶厂|农业局|面店|幼儿园|甜品店|浴池|乐城|工艺厂|美食城|制衣厂|联社|项目组|气站|游艺厅|\
                  屋|包装厂|会所|房|修配厂|分院|用品厂|场|皮具厂|库|超市|淀粉厂|行|车间|食坊|察院|鞋厂|茶艺馆|福利院|电业局|茶馆|经理部|阁|\
                  公园|合会|设计院|检验所|海关|出版局|监狱|歌厅|屠宰场|咖啡屋|药厂|服装厂|公寓|商行|纸品厂|美发店|饰品厂|猪场|\
                  纸箱厂|气象局|医务室|林场|保障局|回收站|养老院|首饰厂|石化|广场|电器厂|招待所|茶店|旅游局|销售部|石料厂|器材厂|茶楼|基地|石矿|\
                  总厂|党校|环保局|有限|糖厂|理发店|公用局|汽修厂|加油点|血站|分部|储备库|报社|发展局|水产局|玩具厂|厅|筹建处|火烧店|电站|彩印厂|\
                  花厂|学校|门业|企业|门市|网吧|山庄|农家乐|苗圃|药铺|业|牧业|药业|商会)(?:\b|（|\()", company, re.S)
            if not company_legal:
                if self.del_style == 1:
                    print("copy:", company)
                    insert_sql = 'insert into table_name_del values ({})'.format(','.join(['%s'] * len(each)))
                    insert_sql = insert_sql.replace('table_name',self.table_name)
                    self.client.update_many(insert_sql, [each])
                print("delete:",company)
                delete_sql="delete from {} where id={}".format(self.table_name,id)
                self.client.delete(delete_sql)

    def del_specific_end(self,removes=['项目','工程','号']):
        # 清除特定字符结尾的名称，如项目名:...项目、工程，因为明显不是公司名
        content_list = self.get_companys()
        if self.del_style == 1:
            content_list = self.get_all()
        for content in content_list:
            id = content[0]
            # print("=========={}=========".format(id))
            company = content[1]
            if self.del_style == 1:
                company = content[self.company_index]
            for remove in removes:
                pattern = remove+'(?:（\w+）|\b)$'
                if re.findall(pattern, company.strip(), re.S):
                    if self.del_style == 1:
                        print("copy:", company)
                        insert_sql = 'insert into table_name_del values ({})'.format(','.join(['%s'] * len(content)))
                        insert_sql = insert_sql.replace('table_name', self.table_name)
                        self.client.update_many(insert_sql, [content])
                    print("delete:", company)
                    delete_sql = "delete from {} where id={}".format(self.table_name,id)
                    self.client.delete(delete_sql)
                    break

    def del_unnormal_length(self,min_leng=3,max_length=40):
        # 删除过长或过短的公司名，包括空字符，默认设置min_leng=5,max_length=40
        content_list = self.get_companys()
        if self.del_style == 1:
            content_list = self.get_all()
        for content in content_list:
            id = content[0]
            # print("=========={}=========".format(id))
            company = content[1]
            if self.del_style == 1:
                company = content[self.company_index]
            lens=len(company.strip())
            if lens > max_length or lens < min_leng:
                if self.del_style == 1:
                    print("copy:", company)
                    insert_sql = 'insert into table_name_del values ({})'.format(','.join(['%s'] * len(content)))
                    insert_sql = insert_sql.replace('table_name', self.table_name)
                    self.client.update_many(insert_sql, [content])
                print("delete:", company)
                delete_sql = "delete from {} where id={}".format(self.table_name,id)
                self.client.delete(delete_sql)

    def del_unnormal_word(self,removes=['项目名称','电子邮箱','公司名称','完成了','，','我局','环评单位','监测机构','.doc','建设工期']):
        # 删除存在特殊文字或符号的公司，如“完才了”
        content_list = self.get_companys()
        if self.del_style == 1:
            content_list = self.get_all()
        for content in content_list:
            id = content[0]
            company = content[1]
            if self.del_style == 1:
                company = content[self.company_index]
            for remove in removes:
                if remove in company:
                    if self.del_style == 1:
                        print("copy:", company)
                        insert_sql = 'insert into table_name_del values ({})'.format(','.join(['%s'] * len(content)))
                        insert_sql = insert_sql.replace('table_name', self.table_name)
                        self.client.update_many(insert_sql, [content])
                    print("delete:", company)
                    delete_sql = "delete from {} where id={}".format(self.table_name,id)
                    self.client.delete(delete_sql)
                    break

    def del_other(self):
        #  年月日公司德格县自来水公司   准格尔旗纳日松镇红进塔村东4km、边贾线K0+23km处
        content_list = self.get_companys()
        if self.del_style == 1:
            content_list = self.get_all()
        for content in content_list:
            id = content[0]
            # print("=========={}=========".format(id))
            company = content[1]
            if self.del_style == 1:
                company = content[self.company_index]
            if re.findall('\d+月\d+日\w+',company,re.S):
                if self.del_style == 1:
                    print("copy:", company)
                    insert_sql = 'insert into table_name_del values ({})'.format(','.join(['%s'] * len(content)))
                    insert_sql = insert_sql.replace('table_name', self.table_name)
                    self.client.update_many(insert_sql, [content])
                print('delete:', company)
                delete_sql = "delete from {} where id={}".format(self.table_name, id)
                self.client.delete(delete_sql)
            elif re.findall('\d+(?:m|km)',company,re.S):
                if self.del_style == 1:
                    print("copy:", company)
                    insert_sql = 'insert into table_name_del values ({})'.format(','.join(['%s'] * len(content)))
                    insert_sql = insert_sql.replace('table_name', self.table_name)
                    self.client.update_many(insert_sql, [content])
                print('delete:', company)
                delete_sql = "delete from {} where id={}".format(self.table_name, id)
                self.client.delete(delete_sql)

    def run(self):
        #主函数，调用所需其他方法
        print("start function:",'update_empty_company' )
        self.update_empty_company()
        print("start function:", 'update_bract')
        self.update_bract()
        print("start function:", 'update_extra_information')
        self.update_extra_information()
        print("start function:", 'update_other')
        self.update_other()
        print("start function:", 'del_empty')
        self.del_empty()
        print("start function:", 'del_specific_end')
        self.del_specific_end()
        print("start function:", 'del_unnormal_end')
        self.del_unnormal_end()
        print("start function:", 'del_unnormal_length')
        self.del_unnormal_length()
        print("start function:", 'del_unnormal_word')
        self.del_unnormal_word()
        print("start function:", 'del_other')
        self.del_other()







'''。建设工期：
准格尔旗纳日松镇红进塔村东4km、边贾线K0+23km处。
吉安市泰和县塘洲镇上洲村西北侧约400m处
沁水县河头村西南80m处、S331省道北侧
东莞市中堂镇环境监测管理系统工程（凤冲河道自动水质监测站）

河津市物资再生利用公司李家庄回收网点
'''


if __name__ == "__main__":
    # test_company()
    obj = CompanyClear(db_config,'hp_dengji_project_info')
    obj.run()









