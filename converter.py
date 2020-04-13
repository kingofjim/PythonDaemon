from funcs import search_query_belong, write_log
import datetime
from socket import inet_ntoa
from struct import pack


class Converter:

    def __init__(self, data, db, debug):
        self.data = data
        self.db = db
        self.debug = debug
        {
            'ns': self.dns,
            'nx': self.nginx,
            'touch': self.touch,
            'nxl': self.touch_service,
            'nsc': self.touch_service,
            'nxc': self.touch_service,
            'nxr': self.touch_reload,
            'nsr': self.touch_reload,
            'sysinfo': self.touch_sys,
        }[data['mt']]()

    def nginx(self):
        domain = search_query_belong(self.data['mt'], self.db, self.data)

        if self.debug:
            print("query: ", self.data['query'])
            print("dest: ", domain)

        dt = datetime.datetime.fromtimestamp(self.data['ts'])
        timestamp = int(dt.timestamp())

        cursor = self.db.dns.cursor()
        cursor.execute('update cdn_ip_list set touch = "%s", service_touch = %s where (ip = "%s" or internal_ip = "%s") and status != 9' % (timestamp, timestamp, self.data['client_ip'], self.data['client_ip'] ))
        self.db.dns.commit()
        write_log('query.log', '%s [Logs] query Agent [-WEB-] : %s [%s - %s] %s [%s]:%s of:%s\n' %
                  (dt.strftime('%Y-%m-%d %H:%M:%S'), self.data['client_ip'], self.data['country'], self.data['city'], self.data['query'], domain, inet_ntoa(pack("!L", self.data['addr'])), self.data['offset']))

        cursor = self.db.logs.cursor()
        cursor.execute('select domain, sendbyte from cdn_logs.cdn_web_logs_%s where date = "%s" and hour = %s and domain = "%s"' % (dt.strftime('%Y%m'), dt.strftime('%Y-%m-%d'), dt.hour, self.data['query']))
        select_result = cursor.fetchone()

        if self.debug:
            print("select_result: ", select_result)

        if select_result is None:
            cursor.execute('insert into cdn_web_logs_%s (domain, sendbyte, date, hour) value ("%s", %s, "%s", %s);' % (dt.strftime('%Y%m'), self.data['query'], self.data['byte'], dt.strftime('%Y-%m-%d'), dt.hour))
        else:
            cursor.execute('update cdn_web_logs_%s set sendbyte = sendbyte + %s  where date = "%s" and hour = %s and domain = "%s"' % (dt.strftime('%Y%m'), self.data['byte'], dt.strftime('%Y-%m-%d'), dt.hour, self.data['query']))
        self.db.logs.commit()


    def dns(self):
        domain = search_query_belong(self.data['mt'], self.db, self.data)

        if self.debug:
            print("query: ", self.data['q'])
            print("dest: ", domain)

        dt = datetime.datetime.fromtimestamp(self.data['ts'])
        timestamp = int(dt.timestamp())

        cursor = self.db.dns.cursor()
        cursor.execute('update cdn_ip_list set touch = "%s", service_touch = %s where (ip = "%s" or internal_ip = "%s") and status != 9' % (timestamp, timestamp, self.data['client_ip'], self.data['client_ip']))
        self.db.dns.commit()
        write_log('query.log', '%s [Logs] query [-DNS-] : %s [%s] %s %s [%s]:%s\n' %
                  (dt.strftime('%Y-%m-%d %H:%M:%S'), self.data['qz'], self.data['qt'], self.data['client_ip'], self.data['q'], domain, str(self.data['offset'])))

        cursor = self.db.logs.cursor()
        cursor.execute('select domain, count from cdn_logs.cdn_dns_logs_%s where date="%s" and domain="%s" and hour = "%s";' % (dt.strftime('%Y%m'), dt.strftime('%Y-%m-%d'), self.data['q'], dt.hour))
        select_result = cursor.fetchone()

        if self.debug:
            print("select_result: ", select_result)

        if select_result is None:
            cursor.execute('insert into cdn_dns_logs_%s (domain, count, date, hour) value ("%s", 1, "%s", %s);' % (dt.strftime('%Y%m'), self.data['q'], dt.strftime('%Y-%m-%d'), dt.hour))
        else:
            cursor.execute('update cdn_dns_logs_%s set count = count+1 where date = "%s" and hour = %s and domain = "%s"' % (dt.strftime('%Y%m'), dt.strftime('%Y-%m-%d'), dt.hour, self.data['q']))

    def touch(self):
        dt = datetime.datetime.now()
        ts = int(dt.timestamp())
        client_ip = self.data['client_ip']

        write_log('query.log', '%s [Logs] Heart rate touch : %s %s\n' % (dt.strftime('%Y-%m-%d %H:%M:%S'), client_ip, ts))
        # logprintf($this -> logfile, "%s %s Heart rate touch : %s %s\n", $datetime, $this -> tag, $sip, $nowts);
        # "2020-04-04 16:13:58 [Logs] Heart rate touch : 172.17.2.6 1585988038"

        cursor = self.db.dns.cursor()
        cursor.execute('update cdn_ip_list set touch = "%s" where (ip = "%s" or internal_ip = "%s") and status != 9' % (ts, client_ip, client_ip))
        self.db.dns.commit()

        # $r = $db -> query("update cdn_ip_list set touch = '$nowts' where (ip = '$sip' or internal_ip = '$sip') and status != 9");


    def touch_service(self):
        dt = datetime.datetime.now()
        ts = int(dt.timestamp())
        client_ip = self.data['client_ip']

        status = 1 if self.data['st'] == 'ok' else 0

        write_log('query.log', '%s [Logs] Service touch %s : %s %s %s\n' % (dt.strftime('%Y-%m-%d %H:%M:%S'), self.data['mt'], client_ip, ts, self.data['st']))

        cursor = self.db.dns.cursor()
        cursor.execute('update cdn_ip_list set service = "%s", service_touch = %s where (ip = "%s" or internal_ip = "%s") and status != 9' % (status ,ts, client_ip, client_ip))
        self.db.dns.commit()

    def touch_reload(self):
        dt = datetime.datetime.now()
        ts = int(dt.timestamp())
        client_ip = self.data['client_ip']
        state = 1 if self.data['reload'] == 'ok' else 0

        cursor = self.db.dns.cursor()
        cursor.execute('update cdn_ip_list set reload = "%s", touch = "%s" where (ip = "%s" or internal_ip = "%s") and status = 1' % (state, ts, client_ip, client_ip))
        self.db.dns.commit()

    def touch_sys(self):
        dt = datetime.datetime.now()
        ts = int(dt.timestamp())
        client_ip = self.data['client_ip']

        write_log('query.log', '%s [Logs] Agent info host : %s  v%s\n' % (dt.strftime('%Y-%m-%d %H:%M:%S'), client_ip, self.data['info']['v']))
        if(self.data['info']['type'] == 'inet'):
            eva = int((int(self.data['info']['TX']) + int(self.data['info']['RX'])) / 1000)
            cursor = self.db.dns.cursor()
            cursor.execute('update cdn_ip_list set eva = %s, touch = "%s" where (ip = "%s" or internal_ip = "%s") and status != 9' % (eva, ts, client_ip, client_ip))
            self.db.dns.commit()

