import config
import pymysql


class DB:
    def __init__(self):
        self.con = pymysql.connect(host=config.HOST, user=config.USER, password=config.PASSWORD,
                                   database=config.DATABASE)
        self.cur = self.con.cursor()

    def get_count(self):
        query = f"SELECT COUNT(*) FROM  {config.TABLE}"
        self.cur.execute(query)
        data = self.cur.fetchone()
        return data[0]

    def get_domains(self, limit: int, offset: int):
        self.reconnect()
        query = f"SELECT website FROM {config.TABLE} LIMIT {limit} OFFSET {offset}"
        self.cur.execute(query)
        data = self.cur.fetchall()
        return [el[0] for el in data]

    def get_null_domains(self, limit: int, offset: int):
        self.reconnect()
        query = f"SELECT website FROM {config.TABLE} WHERE title='' AND description='' LIMIT {limit} OFFSET {offset}"
        self.cur.execute(query)
        data = self.cur.fetchall()
        return [el[0] for el in data]

    def reconnect(self):
        self.cur.close()
        self.con.close()
        self.con = pymysql.connect(host=config.HOST, user=config.USER, password=config.PASSWORD,
                                   database=config.DATABASE)
        self.cur = self.con.cursor()

    def change_title_and_description(self, domain, title, description, date_updated):
        query = "UPDATE alldomains1 SET title=%s, description=%s, date_updated=%s WHERE website=%s"
        self.cur.execute(query, (title, description, date_updated, domain,))
        self.con.commit()

    def update_many(self, data_list=None):
        self.reconnect()
        query = ""
        values = []

        for data_dict in data_list:
            if not query:
                columns = ', '.join('`{0}`'.format(k) for k in data_dict)
                duplicates = ', '.join('{0}=VALUES({0})'.format(k) for k in data_dict)
                place_holders = ', '.join('%s'.format(k) for k in data_dict)
                query = "INSERT INTO alldomains1 ({0}) VALUES ({1})".format(columns, place_holders)
                query = "{0} ON DUPLICATE KEY UPDATE {1}".format(query, duplicates)
            v = [item for key, item in data_dict.items()]
            values.append(v)
        self.cur.executemany(query, values)
        self.con.commit()

    def get_null_count(self):
        query = f"SELECT COUNT(*) FROM  {config.TABLE} WHERE title='' AND description=''"
        self.cur.execute(query)
        data = self.cur.fetchone()
        return data[0]