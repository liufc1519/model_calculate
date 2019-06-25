import pymysql


class MysqlClient(object):
    def __init__(self, db_config):
        try:
            self.sql = u""
            self.host = db_config['host']
            self.port = db_config['port']
            self.user = db_config['user']
            self.passwd = db_config['passwd']
            self.db = db_config['database']
            self.connect = pymysql.connect(host=self.host, port=self.port, user=self.user,
                                           passwd=self.passwd, db=self.db,charset='utf8')
            self.cursor = self.connect.cursor()
        except pymysql.Error as e:
            print(e)
    def query(self, sql,size=0):
        self.cursor.execute(sql)
        if size == 1:
            return self.cursor.fetchone()
        return self.cursor.fetchall()
    def insert(self,sql):
        """
        执行数据库更新操作，如插入等
        """
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)
            return False
    def delete(self,sql):
        """
        执行数据库更新操作，如插入等
        """
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)
            return False
    def update(self,sql):
        """
        执行数据库更新操作，如插入等
        """
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)
            return False
    def update_many(self, sql,content_list):
        try:
            self.cursor.executemany(sql, content_list)
            self.connect.commit()
            return True
        except Exception as e:
            print(e)
            self.connect.rollback()
            return False
    def re_connect(self):
        self.connect = pymysql.connect(host=self.host, port=self.port, user=self.user,
                                       passwd=self.passwd, db=self.db, charset='utf8')
        self.cursor = self.connect.cursor()
    def __del__(self):
        self.connect.close()




if __name__ == "__main__":
    pass










