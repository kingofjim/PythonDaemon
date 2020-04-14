import MySQLdb, configparser


class Database:

    def __init__(self):
        conf = configparser.ConfigParser()
        conf.read('conf.ini')
        self.dns = MySQLdb.connect(conf['mysql-dns']['host'], conf['mysql-dns']['username'], conf['mysql-dns']['pass'], conf['mysql-dns']['database'])
        self.logs = MySQLdb.connect(conf['mysql-logs']['host'], conf['mysql-logs']['username'], conf['mysql-logs']['pass'], conf['mysql-logs']['database'])



        # db = MySQLdb.connect('localhost', 'root', 'jim0703', 'cdn_dns')
        # cursor = db.cursor()
        # cursor.execute('SHOW TABLES;')
        # result = cursor.fetchall()

    def get_candidate_domains(self):
        cur = self.dns.cursor()
        cur.execute('select domain_name from cdn_domains where status = 0 and domain_type in (1, 2);')
        return tuple(x[0] for x in cur.fetchall())


    def get_verified_domains(self):
        cur = self.dns.cursor()
        cur.execute('SELECT id, domain_name, domain_type FROM cdn_domains WHERE status = 0 and domain_type = 3')
        return tuple(x[1] for x in cur.fetchall())