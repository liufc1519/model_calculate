import pymysql
import re
from setting import db_config
from database_api import MysqlClient


client = MysqlClient(db_config)
sql = 'select risk_rank from huanping where risk_rank is not null'
rs = client.query(sql)
risk_list = {each[0] for each in rs}
print(risk_list)




if __name__ == '__main__':
    pass