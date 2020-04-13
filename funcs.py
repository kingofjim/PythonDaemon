import configparser, datetime


def write_log(dest, text):
    conf = configparser.ConfigParser()
    conf.read('conf/conf.ini')
    with open(conf['log']['dir'] + dest, 'a') as f:
        f.write(text)

def write_app_log(text):
    with open('log/error.log', 'a+') as f:
        f.write(text)


def search_query_belong(type, db, data):
    domain = ''
    search_domain = db.get_candidate_domains()
    query = data['q'] if 'q' in data else data['query']
    query = query.lower()
    query_seg = query.split('.')
    dt = datetime.datetime.fromtimestamp(data['ts'])

    if len(query_seg) < 3:
        return query
        # if type == 'ns':
        #     cursor = db.logs.cursor()
        #     temp = 'insert into cdn_dns_logs_%s (domain, count, date, hour) value ("%s", 1, "%s", %s);' % (
        #         dt.strftime('%Y%m'), query, dt.strftime('%Y-%m-%d'), dt.hour)
        #     cursor.execute(temp)
        #     db.logs.commit()
        # elif type == 'nx':
        #     cursor = db.logs.cursor()
        #     temp = 'insert into cdn_web_logs_%s (domain, sendbyte, date, hour) value ("%s", %s, "%s", "%s"););' % (dt.strftime('%Y%m'), query, data['query'], dt.strftime('%Y-%m-%d'), dt.hour)
        #     cursor.execute(temp)
        #     db.logs.commit()
    else:
        if type == 'ns':
            write_log('query.log', '%s [Logs] query [-DNS-] : %s\n' % (dt.strftime('%Y-%m-%d'), query))
        elif type == 'nx':
            write_log('query.log', '%s [Logs] query [-WEB-] : %s\n' % (dt.strftime('%Y-%m-%d'), query))

        # check if cname hosting
        verified_domains = db.get_verified_domains()
        suffix = query_seg[-2] + '.' + query_seg[-1]
        if suffix in verified_domains:
            prefix_domain = query.replace('.'+suffix, '')
            write_log('query.log', "%s [Logs] query string after cutting: '%s' \n" % (dt.strftime('%Y-%m-%d'), prefix_domain))

            # check if wild card
            temp = prefix_domain.split('.')
            temp[0] = '*'
            wildcard = '.'.join(temp)
            if wildcard in search_domain:
                return wildcard

            # check if exactly mapping
            if prefix_domain in search_domain:
                return prefix_domain

            return suffix

        # check if hosted domain
        if suffix in search_domain:
            return suffix

        # check if query match
        temp = query_seg
        temp[0] = '*'
        wildcard = '.'.join(temp)
        if wildcard in search_domain:
            return wildcard

        if query in search_domain:
            return query

        # just record this traffic
        # if type == 'ns':
        #     cursor = db.logs.cursor()
        #     cursor.execute('select domain, sum(count) from cdn_logs.cdn_dns_logs_%s where date="%s" and domain="%s";' % (dt.strftime('%Y%m'), dt.strftime('%Y-%m-%d'), query))
        #     select_result = cursor.fetchone()
        #     if select_result[0] is None:
        #         cursor.execute('insert into cdn_dns_logs_%s (domain, count, date, hour) value ("%s", 1, "%s", %s);' % (dt.strftime('%Y%m'), query, dt.strftime('%Y-%m-%d'), dt.hour))
        #     else:
        #         cursor.execute('update cdn_dns_logs_%s set count = count+1 where date = "%s" and hour = %s and domain = "%s"' % (dt.strftime('%Y%m'), dt.strftime('%Y-%m-%d'), dt.hour, query))
        #     db.logs.commit()
        #
        # elif type == 'nx':
        #     cursor = db.logs.cursor()
        #     cursor.execute('select domain, sum(sendbyte) as sendbyte from cdn_logs.cdn_web_logs_%s where date = "%s" and hour = %s and domain = "%s"' % (dt.strftime('%Y%m'), dt.strftime('%Y-%m-%d'), dt.hour, query))
        #     select_result = cursor.fetchone()
        #     print(dt)
        #     if select_result[0] is None:
        #         cursor.execute('insert into cdn_web_logs_%s (domain, sendbyte, date, hour) value ("%s", %s, "%s", %s);' % (dt.strftime('%Y%m'), query, data['byte'], dt.strftime('%Y-%m-%d'), dt.hour))
        #     else:
        #         cursor.execute('update cdn_web_logs_%s set sendbyte = sendbyte + %s  where date = "%s" and hour = %s and domain = "%s"' % (dt.strftime('%Y%m'), data['byte'], dt.strftime('%Y-%m-%d'), dt.hour, query))
        #     db.logs.commit()

    domain = query

    return domain
