import MySQLdb, configparser
from datetime import datetime
from funcs import write_app_log


class Database:

    def __init__(self):
        conf = configparser.RawConfigParser()
        conf.read('conf.ini')
        self.dns = MySQLdb.connect(conf['mysql-dns']['host'], conf['mysql-dns']['username'], conf['mysql-dns']['pass'], conf['mysql-dns']['database'])
        self.logs = MySQLdb.connect(conf['mysql-logs']['host'], conf['mysql-logs']['username'], conf['mysql-logs']['pass'], conf['mysql-logs']['database'], charset='utf8mb4')

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

    def update_web_record(self, month_year, sendbyte, count, id, force=False):
        cur = self.logs.cursor()
        if force:
            query = 'update cdn_web_logs_%s set sendbyte = %s, count = %s, bandwidth= %s/3600 where id=%s' % (month_year, sendbyte, count, sendbyte, id)
        else:
            query = 'update cdn_web_logs_%s set sendbyte = sendbyte + %s, count = count+ %s where id=%s' % (month_year, sendbyte, count, id)
        # print(query)
        cur.execute(query)
        self.logs.commit()
        return cur.lastrowid

    def insert_dns_record(self, year_month, domain, date, hour, count):
        cur = self.logs.cursor()
        query = 'insert into cdn_dns_logs_%s (domain, date, hour, count) value ("%s", "%s", %s, %s);' % (year_month, domain, date, hour, count)
        # print(query)
        cur.execute(query)
        # self.logs.commit()
        return cur.lastrowid

    def insert_dns_query_record(self, year_month, ip, domain, query, date, hour, count):
        cur = self.logs.cursor()
        query = 'insert into cdn_dns_query_logs_%s (ip, domain, query, date, hour, count) value ("%s", "%s", "%s", "%s", "%s", %s);' % (year_month, ip, domain, query, date, hour, count)
        # print(query)
        cur.execute(query)
        # self.logs.commit()
        # return cur.lastrowid

    def update_dns_record(self, month_year, count, id, force=False):
        cur = self.logs.cursor()
        if force:
            query = 'update cdn_dns_logs_%s set count = %s where id=%s' % (month_year, count, id)
        else:
            query = 'update cdn_dns_logs_%s set count = count+ %s where id=%s' % (month_year, count, id)
        # print(query)
        cur.execute(query)

    def update_dns_query_record(self, month_year, count, id):
        cur = self.logs.cursor()
        query = 'update cdn_dns_query_logs_%s set count = count+ %s where id=%s' % (month_year, count, id)
        # print(query)
        cur.execute(query)

    def get_cdn_web_logs(self, year_month, date, hour):
        if not self.check_table_exist('cdn_web_logs_%s' % year_month):
            self.create_tale('web', year_month)
            write_app_log('%s new table cdn_web_logs_%s created \n' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), year_month))
            return {}

        cur = self.logs.cursor()
        query = 'select domain, id, sendbyte, count from cdn_web_logs_%s where date="%s" and hour=%s;' % (year_month, date, hour)
        cur.execute(query)
        return {x[0]: {"id": x[1], "sendbyte": x[2], "count": x[3]} for x in cur.fetchall()}

    def get_dns_logs(self, year_month, date, hour):
        if not self.check_table_exist('cdn_dns_logs_%s' % year_month):
            self.create_tale('dns', year_month)
            write_app_log('%s new table cdn_dns_logs_%s created \n' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), year_month))
            return {}
        cur = self.logs.cursor()
        query = 'select domain, id, count from cdn_dns_logs_%s where date="%s" and hour=%s;' % (year_month, date, hour)
        cur.execute(query)
        return {x[0]: {"id": x[1], "count": x[2]} for x in cur.fetchall()}

    def get_current_dns_query_record(self, year_month, date, hour):
        if not self.check_table_exist('cdn_dns_query_logs_%s' % year_month):
            self.create_tale('dns_query', year_month)
            write_app_log('%s new table cdn_dns_query_logs_%s created \n' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), year_month))
            return {}
        cur = self.logs.cursor()
        query = 'select id, ip, query from cdn_dns_query_logs_%s where date="%s" and hour=%s;' % (year_month, date, hour)
        cur.execute(query)
        result = {}
        for data in cur.fetchall():
            if data[1] in result:
                result[data[1]][data[2]] = data[0]
            else:
                result[data[1]] = {data[2]: data[0]}
        return result

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
            write_app_log('%s new table cdn_web_distribution_logs_%s created\n' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), year_month))

        cur = self.logs.cursor()
        query = 'insert into cdn_web_distribution_logs_%s (domain, date, hour, country, city, count) value %s;' % (year_month, data)
        # print(query)
        # exit()
        cur.execute(query)
        self.logs.commit()

    def insert_status_dist(self, year_month, data):
        if not self.check_table_exist('cdn_web_status_logs_%s' % year_month):
            self.create_tale('status', year_month)
            write_app_log('%s new table cdn_web_status_logs_%s created\n' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), year_month))

        cur = self.logs.cursor()
        query = 'insert into cdn_web_status_logs_%s (domain, date, hour, status, count) value %s;' % (year_month, data)
        # print(query)
        # exit()
        cur.execute(query)
        self.logs.commit()

    def get_cdn_overused(self, p1, p2, request_threshold, traffic_threshold):
        cur = self.logs.cursor()
        p1_year_month = p1.strftime('%Y%m')
        p2_year_month = p2.strftime('%Y%m')
        p1_datetime = p1.strftime('%Y-%m-%d')
        p2_datetime = p2.strftime('%Y-%m-%d')
        p1_hour = p1.hour
        p2_hour = p2.hour

        query = 'select a.domain, a.count as "last.count", b.count as "count", a.sendbyte as "last.sendbyte", b.sendbyte as "sendbyte" from (select domain, count, sendbyte from cdn_web_logs_%s where date="%s" and hour=%s) a left join (select domain, count, sendbyte from cdn_web_logs_%s where date="%s" and hour=%s) b on a.domain=b.domain where a.count < b.count/%s or a.sendbyte < b.sendbyte/%s;' \
                % (p1_year_month, p1_datetime, p1_hour, p2_year_month, p2_datetime, p2_hour, request_threshold, traffic_threshold)
        cur.execute(query)

        result = cur.fetchall()

        return result

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
        elif type == 'status':
            query = "CREATE TABLE `cdn_web_status_logs_%s` (  `id` int(20) NOT NULL AUTO_INCREMENT,  `domain` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '所查詢的網域',  `date` date DEFAULT NULL COMMENT '日期',  `hour` tinyint(4) DEFAULT NULL COMMENT '小時',  `status` smallint(5) unsigned DEFAULT NULL COMMENT 'HTTP Stauts',  `count` bigint(20) unsigned DEFAULT '0' COMMENT '訪問次數',  `crtime` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '寫入資料庫的時間',  `crtimestamp` int(11) DEFAULT NULL COMMENT '寫入資料庫的時間',  PRIMARY KEY (`id`),  KEY `domain` (`domain`),  KEY `date` (`date`),  KEY `hour` (`hour`),  KEY `crtime` (`crtime`),  KEY `crtimestamp` (`crtimestamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;" % year_month
        elif type == 'dns':
            query = "CREATE TABLE `cdn_dns_logs_%s` ( `id` int(20) unsigned NOT NULL AUTO_INCREMENT, `domain` varchar(256) DEFAULT NULL COMMENT '所查詢的網域', `count` int(11) unsigned DEFAULT NULL COMMENT '查詢次數', `date` date DEFAULT NULL COMMENT '日期', `hour` tinyint(2) DEFAULT NULL COMMENT '小時', `zone` varchar(10) DEFAULT NULL COMMENT '從哪個區域來的查詢', `crtime` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '寫入資料庫的時間', `crtimestamp` int(11) unsigned DEFAULT NULL COMMENT '寫入資料庫的時間', PRIMARY KEY (`id`), KEY `domain` (`domain`), KEY `date` (`date`), KEY `hour` (`hour`), KEY `crtime` (`crtime`), KEY `zone` (`zone`), KEY `crtimestamp` (`crtimestamp`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;" % year_month
        elif type == 'dns_query':
            query = "CREATE TABLE `cdn_dns_query_logs_%s` ( `id` int(20) unsigned NOT NULL AUTO_INCREMENT, `ip` varchar(20) DEFAULT NULL, `domain` varchar(256) DEFAULT NULL COMMENT '網域歸屬', `query` varchar(255) DEFAULT NULL COMMENT '所查詢的網域', `date` date DEFAULT NULL COMMENT '日期', `hour` tinyint(2) DEFAULT NULL COMMENT '小時', `count` int(11) unsigned DEFAULT NULL COMMENT '查詢次數', `crtime` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '寫入資料庫的時間', `crtimestamp` int(11) unsigned DEFAULT NULL COMMENT '寫入資料庫的時間', PRIMARY KEY (`id`), KEY `domain` (`domain`), KEY `date` (`date`), KEY `hour` (`hour`), KEY `crtime` (`crtime`), KEY `crtimestamp` (`crtimestamp`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;" % year_month
        if query:
            cur.execute(query)
            self.logs.commit()

    def remove_existed_data(self, dt):
        year_month = dt.strftime('%Y%m')
        date = dt.strftime('%Y-%m-%d')
        hour = dt.hour
        if self.check_table_exist('cdn_web_logs_%s' % year_month):
            cur = self.logs.cursor()
            query = 'delete from cdn_web_logs_%s where date = "%s" and hour = %s;' % (year_month, date, hour)
            write_app_log("%s remove existed data %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), query))
            print(query)
            cur.execute(query)

        if self.check_table_exist('cdn_web_distribution_logs_%s' % year_month):
            cur = self.logs.cursor()
            query = 'delete from cdn_web_distribution_logs_%s where date = "%s" and hour = %s;' % (year_month, date, hour)
            write_app_log("%s remove existed data %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), query))
            print(query)
            cur.execute(query)

        if self.check_table_exist('cdn_web_status_logs_%s' % year_month):
            cur = self.logs.cursor()
            query = 'delete from cdn_web_status_logs_%s where date = "%s" and hour = %s;' % (year_month, date, hour)
            write_app_log("%s remove existed data %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), query))
            print(query)
            cur.execute(query)

        if self.check_table_exist('cdn_dns_logs_%s' % year_month):
            cur = self.logs.cursor()
            query = 'delete from cdn_dns_logs_%s where date = "%s" and hour = %s;' % (year_month, date, hour)
            write_app_log("%s remove existed data %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), query))
            print(query)
            cur.execute(query)
        if self.check_table_exist('cdn_dns_query_logs_%s' % year_month):
            cur = self.logs.cursor()
            query = 'delete from cdn_dns_query_logs_%s where date = "%s" and hour = %s;' % (year_month, date, hour)
            write_app_log("%s remove existed data %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), query))
            print(query)
            cur.execute(query)

        self.logs.commit()

    def close(self):
        self.dns.close()
        self.logs.close()
