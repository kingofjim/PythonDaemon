import configparser, datetime, requests
from datetime import datetime


def write_log(dest, text):
    conf = configparser.ConfigParser()
    conf.read('conf.ini')
    with open(conf['log']['dir'] + dest, 'a') as f:
        f.write(text)

def write_app_log(text):
    with open('storage/logs/app.log', 'a+') as f:
        f.write(text)

def write_error_log(text):
    with open('storage/logs/error.log', 'a+') as f:
        f.write(text)

def write_pid(text):
    with open('pid.log', 'w+') as f:
        f.write(text)

def get_pid():
    with open('pid.log', 'r') as f:
        if iter(f):
            return next(f)

def get_watcher():
    conf = configparser.ConfigParser()
    conf.read('conf.ini')
    return int(conf['watcher']['cdn_limit'])

def mailSupport(mail_title, content):
    conf = configparser.ConfigParser()
    conf.read('conf.ini')

    headers = {"Content-Type": "application/json", "charset": "utf-8"}
    body = '{"mailer_target": "%s","mailer_subject": "落地點請求異常警報", "mailer_title": "%s", "mailer_content": "%s"}' % (conf['watcher']['mail_target'], mail_title, content)
    body = body.encode('utf-8')
    response = requests.post(conf['watcher']['mail_api'], headers=headers, data=body)
    if response.status_code == 201:
        print('Email alert sent.')
        write_app_log("%s [Watcher] email alert sent - %s\n" % mail_title)
    else:
        print('Email alert error!!!')
        error_response = response.content
        print(error_response)
        write_error_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n' + 'Email Alert Error!!! \n' + error_response + '\n')

def wachter_alert_cdn(datetime, domain):
    headers = {"Content-Type": "application/json", "charset": "utf-8"}
    body = '{"overused_domains": [%s]}' % domain
    print(body)
    response = requests.post('https://api.nicecun.com/api/v1/switch-security-cdn', headers=headers, data=body)
    if response.status_code == 200:
        print('Watcher alert CDN')
        write_app_log("%s [Watcher] alert CDN - %s\n" % (datetime, domain))
    else:
        print('Watcher alert Error!!!')
        error_response = response.content
        print(error_response)
        write_error_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n' + '[Watcher] alert CDN Error!!! \n' + error_response + '\n')

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

def determin_domain(target, cdn_domains, not_cdn_domains):
    domain = target.lower()
    search_domain = cdn_domains
    query = target
    query = query.lower()
    query_seg = query.split('.')

    if len(query_seg) < 3:
        return domain
    else:

        # check if cname hosting
        verified_domains = not_cdn_domains
        suffix = query_seg[-2] + '.' + query_seg[-1]
        if suffix in verified_domains:
            prefix_domain = query.replace('.'+suffix, '')

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

    return domain
