import configparser
from funcs import write_app_log, write_error_log, get_pid, determin_domain, mailSupport, wachter_alert_cdn
import sys, traceback, time
from database import Database
import os
from datetime import datetime, timedelta
import threading
from elasticsearch import Elasticsearch
from dateutil.relativedelta import relativedelta
from hurry.filesize import size, si

LAST_WEB_LIST = {}

def start():
    now = datetime.now()
    start_time_main = now
    end_time_main = start_time_main + timedelta(minutes=5)
    if (start_time_main.hour != end_time_main.hour):
        end_time_main = end_time_main.replace(minute=0, second=0, microsecond=0)
    start_time_side = now.replace(minute=0, second=0, microsecond=0)
    end_time_side = now.replace(minute=0, second=0, microsecond=0)
    end_time_side = end_time_side + timedelta(hours=1)
    timer_side = end_time_side.replace(minute=13)
    start_time_validate = start_time_side
    end_time_validate = end_time_side
    timer_validate = end_time_validate.replace(minute=17)

    write_app_log('Daemon Start at: ' + now.strftime('%Y-%m-%d %H:%M:%S') + '\n')
    print("Main Time: %s %s %s" % (start_time_main, end_time_main, end_time_main))
    print("Side Time: %s %s %s" % (start_time_side, end_time_side, timer_side))
    print("Validate Time: %s %s %s" % (start_time_validate, end_time_validate, timer_validate))

    while (True):
        try:
            print(now)

            if end_time_main <= now:
            # if True:
            # if False:

                # check table exist
                db = Database()
                if(len(db.get_all_table(start_time_main.strftime("%Y%m"))) != len(db.table_set)):
                    create_log_table(start_time_main.month)
                db.close()

                main_cdn_job = threading.Thread(target=job_nginx_main(start_time_main, end_time_main))
                main_dns_job = threading.Thread(target=job_dns_main(start_time_main, end_time_main))
                # main_cdn_job = threading.Thread(target=job_nginx_main(datetime.strptime("2020-12-13 12:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2020-12-13 12:05:00", "%Y-%m-%d %H:%M:%S"), validate=False))
                # main_dns_job = threading.Thread(target=job_dns_main(datetime.strptime("2020-08-28 16:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2020-09-14 17:00:00", "%Y-%m-%d %H:%M:%S"), validate=True))

                start_time_main = end_time_main
                end_time_main = end_time_main + timedelta(minutes=5)
                if (start_time_main.hour != end_time_main.hour):
                    end_time_main = end_time_main.replace(minute=0, second=0, microsecond=0)

                main_cdn_job.start()
                main_dns_job.start()
                main_cdn_job.join()
                main_dns_job.join()

            if timer_side <= now:
            # if(True):
                side_job = threading.Thread(target=job_nginx_side(start_time_side, end_time_side))
                side_job.start()

                start_time_side = end_time_side
                end_time_side = end_time_side + timedelta(hours=1)
                timer_side = timer_side + timedelta(hours=1)
                side_job.join()

            if timer_validate <= now:
            # if True:
                print("%s [Validator] Job Start" % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                main_cdn_job = threading.Thread(target=job_nginx_main(start_time_validate, end_time_validate, validate=True))
                main_dns_job = threading.Thread(target=job_dns_main(start_time_validate, end_time_validate, validate=True))
                # main_cdn_job = threading.Thread(target=job_nginx_main(datetime.strptime("2020-08-28 16:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2020-08-28 17:00:00", "%Y-%m-%d %H:%M:%S"), validate=True))
                # main_dns_job = threading.Thread(target=job_dns_main(datetime.strptime("2020-08-28 16:00:00", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2020-08-28 17:00:00", "%Y-%m-%d %H:%M:%S"), validate=True))

                start_time_validate = end_time_validate
                end_time_validate = end_time_validate + timedelta(hours=1)
                timer_validate = timer_validate + timedelta(hours=1)
                print("Validate Time: %s %s %s" % (start_time_validate, end_time_validate, timer_validate))

                main_cdn_job.start()
                main_dns_job.start()
                main_cdn_job.join()
                main_dns_job.join()

        except KeyboardInterrupt:
            pass
        except Exception as e:
            error_class = e.__class__.__name__  # 取得錯誤類型
            detail = e.args[0]  # 取得詳細內容
            cl, exc, tb = sys.exc_info()  # 取得Call Stack

            errMsg = ''
            for lastCallStack in traceback.extract_tb(tb):
                fileName = lastCallStack[0]  # 取得發生的檔案名稱
                lineNum = lastCallStack[1]  # 取得發生的行號
                funcName = lastCallStack[2]  # 取得發生的函數名稱
                errMsg += "File \"{}\", line {}, in {}: [{}] {}\n".format(fileName, lineNum, funcName, error_class, detail)

            dt = datetime.now()
            write_error_log(("%s\n + %s\n") % (dt.strftime('%Y-%m-%d %H:%M:%S'), errMsg))
            print(errMsg)
            # mailSupport("PythonDaemon ERROR", errMsg)

        time.sleep(5)
        now = datetime.now()


def kill():
    pid = get_pid()
    os.popen('kill '+ pid).read().strip()

def job_nginx_main(start_time, end_time, validate=False):
    job_start_time = datetime.now()
    print("%s [Web-Main] Job Start" % job_start_time.strftime('%Y-%m-%d %H:%M:%S'))
    global LAST_WEB_LIST
    last_web_list = LAST_WEB_LIST
    db = Database()
    start_time_utc = start_time - timedelta(hours=8)
    end_time_utc = end_time - timedelta(hours=8)

    current_web_list = db.get_cdn_web_logs(start_time.strftime('%Y%m'), start_time.date(), start_time.hour)

    conf = configparser.ConfigParser()
    conf.read('conf.ini')

    if conf['app']['force_input_ol_domains'] == 'True':
        # force input OL data for testing
        cdn_domains = ['leacloud.net','qnvtang.com','leacloud.com','reachvpn.com','jetstartech.com','wqlyjy.cn','lea.cloud','jtechcloud.com','leaidc.com','ahskzs.cn','tjwohuite.com','www.ttt.com','hbajhw.com','tjflsk.com','anjuxinxi.com','test.leacloud.net','tongxueqn.com','*.unnychina.com','*.atpython.com','*.daguosz.com','*.cfbaoche.com','*.baliangxian.com','*.yunyishihu.com','*.clwdfhw.com','*.hnstrcyj.com','www.lanshengyoupin.com','www.chinaynt.com','www.pcgame198.com','www.pintusx.com','www.frzhibo.com','www.nanjingcaishui.com','www.shsxmygs.com','www.jsonencode.com','www.xgmgnz.com','www.sinceidc.com','www.njyymzp.com','www.rikimrobot.com','www.mifeiwangluo.com','www.lvqqtt.com','www.wuhanbsz.com','www.xueqiusj.com','www.queqiaocloud.com','www.jiajiaoshiting.com','www.laotsai.com','www.daoliuliang365.com','www.dazhougongjiao.com','www.hndingkun.com','www.liangct.com','www.amandacasa.com','www.ruiyoushouyou.com','www.yychaoli.com','www.allcureglobal.com','www.whrenatj.com','yidaaaa.com','lea.hncgw.cn','webld.cqgame.games','test12345.tongxueqn.com','cc.kkk222.com','cdn.2019wsd.com','webh5ld.cqgame.cc','vip77759.com','tggame.topgame6.com','*.jcxfdc.cn','*.mrqzs.cn','*.dlswl.cn','*.0oiser.club','*.greenpay.xyz','*.greentrad.net','*.greenpay.vip','www.youxizaixian100.com','yilongth.com','weidichuxing.com','dianwankeji.com','haiyunpush.com','qushiyunmei.com']
        not_cdn_domains = ["gotolcd.net", "adminlcd.net", "highlcd.net", "leaidc.net"]
    else:
        cdn_domains = db.get_cdn_domains()
        not_cdn_domains = db.get_not_cdn_domains()

    print("Main - cdn_domains:", cdn_domains)
    print("Main - not_cdn_domains:", not_cdn_domains)

    period = [start_time_utc.strftime('%Y-%m-%dT%H:%M:%S'), end_time_utc.strftime('%Y-%m-%dT%H:%M:%S')]
    print("[Web-Main] period:", period)
    # period = ['2020-08-24T03:00:00', '2020-08-24T04:00:00']

    elastic = Elasticsearch()
    update_list = elastic.search_sendbtye_by_domains(period=period)
    print("[Web-Main] search_sendbtye_by_domains - result: %s" % update_list)
    if update_list:
        update_data = {}
        validate_list = {}
        for query_domain, val in update_list.items():
            domain = determin_domain(query_domain, cdn_domains, not_cdn_domains)
            sendbyte = int(val[1])
            count = val[0]
            if domain:
                # write_app_log('%s => %s\n' % (query_domain, domain))
                if (domain in current_web_list):
                    id = current_web_list[domain]['id']
                    if validate:
                        if domain in validate_list:
                            validate_list[domain]["sendbyte"] += int(sendbyte)
                            validate_list[domain]["count"] += count
                        else:
                            validate_list[domain] = {}
                            validate_list[domain]["sendbyte"] = int(sendbyte)
                            validate_list[domain]["count"] = count
                            validate_list[domain]["id"] = id
                    else:
                        db.update_web_record(start_time.strftime('%Y%m'), sendbyte, count, id)
                        write_app_log('%s [Web] update: %s[%s] with sendbyte: %s count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), domain, id, sendbyte, count))
                    # print("update", domain, val)
                else:
                    id = db.insert_web_record(start_time.strftime('%Y%m'), domain, start_time.strftime('%Y-%m-%d'), start_time.hour, sendbyte, count)
                    current_web_list[domain] = {}
                    current_web_list[domain]["id"] = id
                    current_web_list[domain]["sendbyte"] = sendbyte
                    current_web_list[domain]["count"] = count
                    write_app_log('%s [Web] insert: %s[%s] with sendbyte: %s count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), domain, id, sendbyte, count))
                    # print("insert", domain, val)

                if domain in update_data:
                    update_data[domain][0] += val[0]
                    update_data[domain][1] += val[1]
                else:
                    update_data[domain] = update_list[query_domain]
            else:
                print('%s is not registered in database.\n' % (query_domain))
        # 事後驗證
        if validate:
            if validate_list and current_web_list:
                for domain, val in validate_list.items():
                    sendbyte = val["sendbyte"]
                    count = val["count"]
                    id = val["id"]
                    if domain in current_web_list and (current_web_list[domain]["sendbyte"] != sendbyte or current_web_list[domain]["count"] != count):
                        db.update_web_record(start_time.strftime('%Y%m'), sendbyte, count, id, force=True)
                        write_app_log('%s [Web] FORCE update: %s[%s] with sendbyte: %s count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), domain, id, sendbyte, count))
        else:
            # 偵測攻擊
            if conf['watcher']['enable'] == 'True':
                # last_web_list = {
                #     "images.cq9web.com": {
                #         "id": 1,
                #         "sendbyte": 100,
                #         "count": 1
                #     },
                #     'www.luntanx18.info': {
                #         "id": 2,
                #         "sendbyte": 1,
                #         "count": 0
                #     }
                # }
                watcher(conf, last_web_list, update_list, start_time, end_time)
    else:
        print("[Web] %s ELK result empty" % start_time.strftime('%Y-%m-%dT%H:%M:%S'))
        mailSupport("ELK查詢空值", "job_nginx_main %s ~ %s" % (start_time.strftime('%Y-%m-%dT%H:%M:%S'), end_time.strftime('%Y-%m-%dT%H:%M:%S')))

    db.close()
    LAST_WEB_LIST = update_list
    print("[Web-Main] Time spend %s" % (datetime.now() - job_start_time).total_seconds())

def job_nginx_side(start_time, end_time):
    job_start_time = datetime.now()
    print("%s [Web-Side] Job Start" % job_start_time.strftime('%Y-%m-%d %H:%M:%S'))
    conf = configparser.ConfigParser()
    conf.read('conf.ini')
    db = Database()
    db.update_web_bandwidth(start_time.strftime('%Y%m'), end_time.strftime('%Y-%m-%d'), end_time.hour)
    write_app_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' bandwidth updated\n')

    els = Elasticsearch()
    start_time_utc = start_time - timedelta(hours=8)
    end_time_utc = end_time - timedelta(hours=8)
    period = [start_time_utc.strftime('%Y-%m-%dT%H:%M:%S'), end_time_utc.strftime('%Y-%m-%dT%H:%M:%S')]

    if conf['app']['force_input_ol_domains'] == 'True':
        # force input OL data for testing
        cdn_domains = ['leacloud.net','qnvtang.com','leacloud.com','reachvpn.com','jetstartech.com','wqlyjy.cn','lea.cloud','jtechcloud.com','leaidc.com','ahskzs.cn','tjwohuite.com','www.ttt.com','hbajhw.com','tjflsk.com','anjuxinxi.com','test.leacloud.net','tongxueqn.com','*.unnychina.com','*.atpython.com','*.daguosz.com','*.cfbaoche.com','*.baliangxian.com','*.yunyishihu.com','*.clwdfhw.com','*.hnstrcyj.com','www.lanshengyoupin.com','www.chinaynt.com','www.pcgame198.com','www.pintusx.com','www.frzhibo.com','www.nanjingcaishui.com','www.shsxmygs.com','www.jsonencode.com','www.xgmgnz.com','www.sinceidc.com','www.njyymzp.com','www.rikimrobot.com','www.mifeiwangluo.com','www.lvqqtt.com','www.wuhanbsz.com','www.xueqiusj.com','www.queqiaocloud.com','www.jiajiaoshiting.com','www.laotsai.com','www.daoliuliang365.com','www.dazhougongjiao.com','www.hndingkun.com','www.liangct.com','www.amandacasa.com','www.ruiyoushouyou.com','www.yychaoli.com','www.allcureglobal.com','www.whrenatj.com','yidaaaa.com','lea.hncgw.cn','webld.cqgame.games','test12345.tongxueqn.com','cc.kkk222.com','cdn.2019wsd.com','webh5ld.cqgame.cc','vip77759.com','tggame.topgame6.com','*.jcxfdc.cn','*.mrqzs.cn','*.dlswl.cn','*.0oiser.club','*.greenpay.xyz','*.greentrad.net','*.greenpay.vip','www.youxizaixian100.com','yilongth.com','weidichuxing.com','dianwankeji.com','haiyunpush.com','qushiyunmei.com']
        not_cdn_domains = ["gotolcd.net", "adminlcd.net", "highlcd.net", "leaidc.net"]
    else:
        cdn_domains = db.get_cdn_domains()
        not_cdn_domains = db.get_not_cdn_domains()

    # -- city count --
    # print("[Web-Side] period:", period)
    # dis_data = els.search_city_count_distribution(period)
    # query_val = ''
    # for query_domain, country_data in dis_data.items():
    #     domain = determin_domain(query_domain, cdn_domains, not_cdn_domains)
    #     if domain:
    #         # write_app_log('%s => %s\n' % (query_domain, domain))
    #         for country, city_data in country_data.items():
    #             for city, count in city_data.items():
    #                 query_val += '("%s","%s","%s","%s","%s","%s"),' % (domain, start_time.strftime('%Y-%m-%d'), start_time.hour, country, city, count)
    #                 write_app_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Web distribution] insert: %s status:%s count:%s\n' % (domain, city, count))
    #     else:
    #         print('[Web-Side] %s is not registered in database.\n' % (query_domain))
    # query_val = query_val[:-1]
    # if query_val:
    #     db.insert_web_dist(start_time.strftime('%Y%m'), query_val)
    # print("[Web-Side] Time spend %s" % (datetime.now() - job_start_time).total_seconds())

    # -- status --
    job_start_time = datetime.now()
    elk_status_data = els.search_status_distribution(period)
    query_val = ''
    for query_domain, status_data in elk_status_data.items():
        domain = determin_domain(query_domain, cdn_domains, not_cdn_domains)
        if domain:
            # write_app_log('%s => %s\n' % (query_domain, domain))
            for status, count in status_data.items():
                query_val += '("%s","%s","%s","%s","%s"),' % (domain, start_time.strftime('%Y-%m-%d'), start_time.hour, status, count)
                write_app_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Web Status] insert: %s status:%s count:%s\n' % (domain, status, count))
        else:
            print('[Web-Status] %s is not registered in database.\n' % (query_domain))

    query_val = query_val[:-1]
    if query_val:
        db.insert_status_dist(start_time.strftime('%Y%m'), query_val)

    db.close()
    print("[Web-Status] Time spend %s" % (datetime.now() - job_start_time).total_seconds())

def job_dns_main(start_time, end_time, validate=False):
    job_start_time = datetime.now()
    print("%s [Web-Main] Job Start" % job_start_time.strftime('%Y-%m-%d %H:%M:%S'))
    db = Database()
    start_time_utc = start_time - timedelta(hours=8)
    end_time_utc = end_time - timedelta(hours=8)
    current_dns_list = db.get_dns_logs(start_time.strftime('%Y%m'), start_time.date(), start_time.hour)

    conf = configparser.ConfigParser()
    conf.read('conf.ini')
    if conf['app']['force_input_ol_domains'] == 'True':
        # force input OL data for testing
        cdn_domains = ['leacloud.net','qnvtang.com','leacloud.com','reachvpn.com','jetstartech.com','wqlyjy.cn','lea.cloud','jtechcloud.com','leaidc.com','ahskzs.cn','tjwohuite.com','www.ttt.com','hbajhw.com','tjflsk.com','anjuxinxi.com','test.leacloud.net','tongxueqn.com','*.unnychina.com','*.atpython.com','*.daguosz.com','*.cfbaoche.com','*.baliangxian.com','*.yunyishihu.com','*.clwdfhw.com','*.hnstrcyj.com','www.lanshengyoupin.com','www.chinaynt.com','www.pcgame198.com','www.pintusx.com','www.frzhibo.com','www.nanjingcaishui.com','www.shsxmygs.com','www.jsonencode.com','www.xgmgnz.com','www.sinceidc.com','www.njyymzp.com','www.rikimrobot.com','www.mifeiwangluo.com','www.lvqqtt.com','www.wuhanbsz.com','www.xueqiusj.com','www.queqiaocloud.com','www.jiajiaoshiting.com','www.laotsai.com','www.daoliuliang365.com','www.dazhougongjiao.com','www.hndingkun.com','www.liangct.com','www.amandacasa.com','www.ruiyoushouyou.com','www.yychaoli.com','www.allcureglobal.com','www.whrenatj.com','yidaaaa.com','lea.hncgw.cn','webld.cqgame.games','test12345.tongxueqn.com','cc.kkk222.com','cdn.2019wsd.com','webh5ld.cqgame.cc','vip77759.com','tggame.topgame6.com','*.jcxfdc.cn','*.mrqzs.cn','*.dlswl.cn','*.0oiser.club','*.greenpay.xyz','*.greentrad.net','*.greenpay.vip','www.youxizaixian100.com','yilongth.com','weidichuxing.com','dianwankeji.com','haiyunpush.com','qushiyunmei.com']
        not_cdn_domains = ["gotolcd.net", "adminlcd.net", "highlcd.net", "leaidc.net"]
    else:
        cdn_domains = db.get_cdn_domains()
        not_cdn_domains = db.get_not_cdn_domains()

    period = [start_time_utc.strftime('%Y-%m-%dT%H:%M:%S'), end_time_utc.strftime('%Y-%m-%dT%H:%M:%S')]
    print("[DNS-Main] period:", period)
    # period = ['2020-06-17T08:20:00', '2020-06-17T08:25:00']

    elastic = Elasticsearch()
    update_list = elastic.search_dns_query_by_domains(period=period)
    validate_list = {}

    if update_list:
        for query_domain, count in update_list.items():
            domain = determin_domain(query_domain, cdn_domains, not_cdn_domains)
            if domain:
                # write_app_log('%s => %s\n' % (query_domain, domain))
                if domain in current_dns_list:
                    id = current_dns_list[domain]["id"]
                    if validate:
                        if domain in validate_list:
                            validate_list[domain]['count'] += count
                        else:
                            validate_list[domain] = {}
                            validate_list[domain]['count'] = count
                            validate_list[domain]['id'] = id
                    else:
                        db.update_dns_record(start_time.strftime('%Y%m'), str(count), id)
                        write_app_log('%s [DNS] update: %s[%s] with count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), domain, id, str(count)))
                else:
                    id = db.insert_dns_record(start_time.strftime('%Y%m'), domain, start_time.strftime('%Y-%m-%d'), start_time.hour, str(count))
                    write_app_log('%s [DNS] insert: %s with count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), domain, str(count)))
                    current_dns_list[domain] = {"id": id, "count": count}
            else:
                print('[DNS-Main] %s is not registered in database.\n' % (query_domain))

        if validate:
            for domain, val in validate_list.items():
                count = val["count"]
                id = val["id"]
                if domain in current_dns_list and count != current_dns_list[domain]["count"]:
                    db.update_dns_record(start_time.strftime('%Y%m'), str(count), id, force=True)
                    write_app_log('%s [DNS] FORCE update: %s[%s] with count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), domain, id, str(count)))

        db.logs.commit()
    else:
        print("[DNS-Main] %s ELK result empty" % start_time.strftime('%Y-%m-%dT%H:%M:%S'))
        mailSupport("ELK查詢空值", "job_dns_main %s ~ %s" % (start_time.strftime('%Y-%m-%dT%H:%M:%S'), end_time.strftime('%Y-%m-%dT%H:%M:%S')))

    print("[DNS-Main] Time spend %s" % (datetime.now()-job_start_time).total_seconds())

    # job_start_time = datetime.now()
    # current_dns_ip_list = db.get_current_dns_query_record(start_time.strftime('%Y%m'), start_time.date(), start_time.hour)
    # update_list = elastic.search_dns_query_by_ip(period=period)
    #
    # for ip, ip_data in update_list.items():
    #     for query_domain, count in ip_data.items():
    #         # print(ip, query_domain, count)
    #         domain = determin_domain(query_domain, cdn_domains, not_cdn_domains)
    #         # print(query_domain, "=> ", domain)
    #         if domain:
    #             # write_app_log('%s => %s\n' % (query_domain, domain))
    #             if (ip in current_dns_ip_list and query_domain in current_dns_ip_list[ip]):
    #                 id = current_dns_ip_list[ip][query_domain]
    #                 db.update_dns_query_record(start_time.strftime('%Y%m'), str(count), id)
    #                 write_app_log('[DNS-IP] %s update: %s %s[%s] with count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), ip, domain, id, str(count)))
    #                 # print("update", domain, count)
    #             else:
    #                 db.insert_dns_query_record(start_time.strftime('%Y%m'), ip, domain, query_domain, start_time.strftime('%Y-%m-%d'), start_time.hour, str(count))
    #                 write_app_log('[DNS-IP] %s insert: %s with count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), domain, str(count)))
    #                 # print("insert", domain, count)
    #         else:
    #             write_app_log('[DNS-IP] %s is not registered in database.\n' % (query_domain))
    #             print('[DNS-IP] %s is not registered in database.\n' % (query_domain))
    # db.logs.commit()
    # print("[DNS-IP] Time spend %s" % (datetime.now() - job_start_time).total_seconds())
    db.close()

def watcher(conf, last_web_list, update_list, start_time, end_time):
    print("%s [Watcher] Start]" % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    write_app_log("%s [Watcher] Start]\n" % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    if last_web_list:
        traffic_max = int(conf["watcher"]["cdn_traffic_max"])
        traffic_min = int(conf["watcher"]["cdn_traffic_min"])
        traffic_multiply = int(conf["watcher"]["cdn_traffic_multiply"])
        request_max = int(conf["watcher"]["cdn_request_max"])
        request_min = int(conf["watcher"]["cdn_request_min"])
        request_multiply = int(conf["watcher"]["cdn_request_multiply"])
        alert_domain_list = {"traffic": {}, "request": {}}
        for domain, data in update_list.items():
            val = {}
            val["count"] = data[0]
            val["sendbyte"] = data[1]
            if domain in last_web_list:
                # check sendbyte
                if val["sendbyte"] > traffic_min:
                    if val["sendbyte"] > traffic_max or val["sendbyte"] > last_web_list[domain][1] * traffic_multiply:
                        alert_domain_list["traffic"][domain] = val
                        alert_domain_list["traffic"][domain]["sendbyte"] = val["sendbyte"]

                elif val["count"] > request_min:
                    if val["count"] > request_max or val["count"] > last_web_list[domain][0] * request_multiply:
                        alert_domain_list["request"][domain] = val

        if alert_domain_list["request"] or alert_domain_list["traffic"]:
            mail_content = ''
            # alert_content = ''

            for cate in alert_domain_list:
                if (cate == "traffic" and alert_domain_list["traffic"]):
                    mail_content += '<h4>流量(Traffic)超標</h4>'
                elif (cate == "request" and alert_domain_list["request"]):
                    mail_content += '<h4>請求(Request)超標</h4>'

                for domain, val in alert_domain_list[cate].items():
                    last_count = last_web_list[domain][0]
                    count = val['count']
                    last_sendbyte = size(last_web_list[domain][1], system=si)
                    sendbyte = size(val['sendbyte'], system=si)
                    mail_content += '%s:<br>&nbsp;&nbsp; Request(次) %s => %s<br>&nbsp;&nbsp; Traffic %s => %s <br>' % (domain, last_count, count, last_sendbyte, sendbyte)
                # alert_content += '"%s",' % domain
            try:
                mail_title = "%s ~ %s" % (start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S'))
                mail_content += "<div><h5>警報標準</h5><p>流量(max): %s</p><p>流量(min): %s</p><p>流量(multiply): %s</p><p>請求(max): %s</p><p>請求(min): %s</p><p>請求(multiply): %s</p></div>" % (size(traffic_max, system=si), size(traffic_min, system=si), traffic_multiply, request_max, request_min, request_multiply)
                mailSupport(mail_title, mail_content, "域名可能遭受攻擊！")
            except Exception as e:
                write_error_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + e.__str__())

            # try:
            #     wachter_alert_cdn(datetime.strftime('%Y-%m-%d %H:%M:%S'), alert_content)
            # except Exception as e:
            #     write_error_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + e.__str__())

def update_period(p1, p2):
    print("Start update period %s ~ %s" % (p1, p2))
    start_time = datetime.now()

    period_list = []
    while p1 < p2:
        new_p1 = p1+timedelta(hours=1)
        period = (p1, new_p1)
        p1 = new_p1
        period_list.append(period)
    for period in period_list:
        db = Database()
        db.remove_existed_data(period[0])
        db.close()
        job_nginx_main(period[0], period[1])
        job_nginx_side(period[0], period[1])
        job_dns_main(period[0], period[1])

    print("%s Completed update period" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("update_period %s ~ %s took %s second(s)" % (p1, p2, (datetime.now() - start_time).total_seconds()))

def create_log_table(month):
    if month is None:
        next_month = datetime.now() + relativedelta(months=1)
    else:
        next_month = datetime.now().replace(month=int(month))
    db = Database()
    db.create_tale("all", next_month.strftime("%Y%m"))
    db.close()

if(len(sys.argv) > 1):
    if sys.argv[1] == 'update_period':
        if len(sys.argv) == 4:
            try:
                p1 = datetime.strptime(sys.argv[2], '%Y-%m-%dT%H:%M:%S')
                p2 = datetime.strptime(sys.argv[3], '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                print('請輸入有效Timestamp ex. update_period 2020-05-04T09:00:00 2020-05-04T18:00:00')
                exit()
            if p1.minute != 0 or p1.second != 00 or p2.minute != 0 or p2.second != 00:
                print('僅接受輸入整點0分0秒 ex. 2020-05-04T09:00:00')
            else:
                eval(sys.argv[1])(p1, p2)

        else:
            print('請輸入有效Timestamp ex. update_period 2020-05-04T09:00:00 2020-05-04T18:00:00')
    elif sys.argv[1] == 'create_log_table':
        eval(sys.argv[1])(month=None if len(sys.argv) != 3 else sys.argv[2])

    elif sys.argv[1] == 'start':
        eval(sys.argv[1])()
    else:
        print('請輸入有效指令\nstart\nupdate_period ${timestamp1} ${timestamp2}\ncreate_log_table')