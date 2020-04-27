import MySQLdb, configparser
from datetime import datetime
from funcs import write_app_log


class Database:

    def __init__(self):
        conf = configparser.RawConfigParser()
        conf.read('conf.ini')
        self.dns = MySQLdb.connect(conf['mysql-dns']['host'], conf['mysql-dns']['username'], conf['mysql-dns']['pass'], conf['mysql-dns']['database'])
        self.logs = MySQLdb.connect(conf['mysql-logs']['host'], conf['mysql-logs']['username'], conf['mysql-logs']['pass'], conf['mysql-logs']['database'], charset='utf8mb4')



        # db = MySQLdb.connect('localhost', 'root', 'jim0703', 'cdn_dns')
        # cursor = db.cursor()
        # cursor.execute('SHOW TABLES;')
        # result = cursor.fetchall()

    def get_cdn_domains(self):
        cur = self.dns.cursor()
        cur.execute('select domain_name from cdn_domains where status = 0 and domain_type in (1, 2);')
        return tuple(x[0] for x in cur.fetchall())


    def get_not_cdn_domains(self):
        cur = self.dns.cursor()
        cur.execute('SELECT domain_name FROM cdn_domains WHERE status = 0 and domain_type = 3')
        return tuple(x[0] for x in cur.fetchall())

    def insert_web_record(self, year_month, domain, date, hour, sendbyte, count):
        cur = self.logs.cursor()
        query = 'insert into cdn_web_logs_%s (domain, date, hour, sendbyte, count) value ("%s", "%s", %s, %s, %s);' % (year_month, domain, date, hour, sendbyte, count)
        # print(query)
        cur.execute(query)
        self.logs.commit()
        return cur.lastrowid

    def update_web_record(self, month_year, sendbyte, count, id):
        cur = self.logs.cursor()
        query = 'update cdn_web_logs_%s set sendbyte = sendbyte + %s, count = count+ %s where id=%s' % (month_year, sendbyte, count, id)
        # print(query)
        cur.execute(query)
        self.logs.commit()

    def get_current_hour_web_record(self, year_month, date, hour):
        # print(self.check_table_exist('cdn_web_logs_%s' % year_month))
        # exit()
        if not self.check_table_exist('cdn_web_logs_%s' % year_month):
            self.create_tale('web', year_month)
            write_app_log('%s new table cdn_web_logs_%s created' % (datetime.now().strftime('%Y-%m-%d %H:%I:%S'), year_month))

        cur = self.logs.cursor()
        query = 'select id, domain from cdn_web_logs_%s where date="%s" and hour=%s;' % (year_month, date, hour)
        cur.execute(query)
        return {x[1]:x[0] for x in cur.fetchall()}

    def update_web_bandwidth(self, year_month, now_date, now_hour):
        if self.check_table_exist('cdn_web_logs_%s' % year_month):
            cur = self.logs.cursor()
            query = 'update cdn_web_logs_%s set bandwidth = sendbyte/3600 where bandwidth = 0 and sendbyte != 0 and not (date="%s" and hour=%s);' % (year_month, now_date, now_hour)
            # print(query)
            cur.execute(query)
            self.logs.commit()

    def insert_web_dist(self, year_month, data):
        if not self.check_table_exist('cdn_web_distribution_logs_%s' % year_month):
            self.create_tale('dist', year_month)
            print('')
            write_app_log('%s new table cdn_web_distribution_logs_%s created' % (datetime.now().strftime('%Y-%m-%d %H:%I:%S'), year_month))

        cur = self.logs.cursor()
        query = 'insert into cdn_web_distribution_logs_%s (domain, date, hour, country, city, count) value %s;' % (year_month, data)
        # print(query)
        # exit()
        cur.execute(query)
        self.logs.commit()

    def check_table_exist(self, tablename):
        cur = self.logs.cursor()
        query = 'SELECT * FROM information_schema.tables WHERE table_name = "%s" and not table_schema like "%%backup"' % tablename
        cur.execute(query)
        # print(tablename)
        # print(cur.fetchall())
        # exit()
        return True if len(cur.fetchall()) == 1 else False

    def create_tale(self, type, year_month):
        cur = self.logs.cursor()
        if type == 'dist':
            query = "CREATE TABLE `cdn_web_distribution_logs_%s` (  `id` int(20) NOT NULL AUTO_INCREMENT,  `domain` varchar(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '所查詢的網域',  `date` date DEFAULT NULL COMMENT '日期',  `hour` tinyint(4) DEFAULT NULL COMMENT '小時',  `country` varchar(40) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '所查詢的國家',  `city` varchar(40) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '所查詢的城市',  `count` bigint(20) unsigned DEFAULT 0 COMMENT '訪問次數',  `crtime` datetime DEFAULT current_timestamp() COMMENT '寫入資料庫的時間',  `crtimestamp` int(11) DEFAULT NULL COMMENT '寫入資料庫的時間',  PRIMARY KEY (`id`),  KEY `domain` (`domain`),  KEY `date` (`date`),  KEY `hour` (`hour`),  KEY `crtime` (`crtime`),  KEY `crtimestamp` (`crtimestamp`))" % year_month
        elif type == 'web':
            query = "CREATE TABLE `cdn_web_logs_%s` (  `id` int(20) NOT NULL AUTO_INCREMENT,  `domain` varchar(256) DEFAULT NULL COMMENT '所查詢的網域',  `date` date DEFAULT NULL COMMENT '日期',  `hour` tinyint(4) DEFAULT NULL COMMENT '小時',  `sendbyte` bigint(20) unsigned DEFAULT '0' COMMENT '流量',  `bandwidth` float unsigned DEFAULT '0' COMMENT '頻寬',  `count` bigint(20) unsigned DEFAULT '0' COMMENT '訪問次數',  `crtime` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '寫入資料庫的時間',  `crtimestamp` int(11) unsigned DEFAULT NULL COMMENT '寫入資料庫的時間',  PRIMARY KEY (`id`),  KEY `domain` (`domain`),  KEY `date` (`date`),  KEY `hour` (`hour`),  KEY `crtime` (`crtime`),  KEY `crtimestamp` (`crtimestamp`)) CHARSET=utf8;" % year_month

        if query:
            cur.execute(query)
            self.logs.commit()

    def close(self):
        self.dns.close()
        self.logs.close()