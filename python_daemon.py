import configparser
from funcs import write_app_log, get_pid, determin_domain
import sys, traceback, time
from database import Database
import os
from datetime import datetime, timedelta
import threading
from elasticsearch import Elasticsearch

def start():
    # db = Database()
    # print(db.create_tale('web', '202005'))
    # els = Elasticsearch()
    # period = ("2020-05-03T06:13:15", "2020-05-03T07:13:15")

    now = datetime.now()
    start_time_main = now
    end_time_main = start_time_main + timedelta(minutes=5)
    start_time_side = now.replace(minute=0, second=0, microsecond=0)
    end_time_side = now.replace(minute=0, second=0, microsecond=0)
    end_time_side = end_time_side + timedelta(hours=1)
    # print(end_time_side)
    # exit()
    # end_time_side + timedelta(minutes=1)

    write_app_log('Daemon Start at: ' + now.strftime('%Y-%m-%d %H:%M:%S') + '\n')
    print("start_time:", start_time_main)
    print("end_time_main:", end_time_main)
    print("end_time_side:", end_time_side)


    try:
        while(True):
            print(now)
            # every 5 mins
            if(end_time_main <= now):
            # if (True):
                write_app_log('Main job start at: ' + now.strftime('%Y-%m-%d %H:%M:%S') + '\n')
                main_job = threading.Thread(target=job_nginx_main(start_time_main, end_time_main))
                main_job.start()

                start_time_main = end_time_main
                end_time_main = end_time_main + timedelta(minutes=5)

            # every 1 hour
            if(end_time_side <= now):
            # if(True):
                write_app_log('Side job start at: ' + now.strftime('%Y-%m-%d %H:%M:%S') + '\n')
                side_job = threading.Thread(target=job_nginx_side(start_time_side, end_time_side))
                side_job.start()
                start_time_side = end_time_side
                end_time_side = end_time_side + timedelta(hours=1)
            # exit()
            time.sleep(10)
            now = datetime.now()
    except Exception as e:
        # print("something wrong")
        error_class = e.__class__.__name__  # 取得錯誤類型
        detail = e.args[0]  # 取得詳細內容
        cl, exc, tb = sys.exc_info()  # 取得Call Stack

        errMsg = ''
        for lastCallStack in traceback.extract_tb(tb):
            fileName = lastCallStack[0]  # 取得發生的檔案名稱
            lineNum = lastCallStack[1]  # 取得發生的行號
            funcName = lastCallStack[2]  # 取得發生的函數名稱
            errMsg += "File \"{}\", line {}, in {}: [{}] {}\n".format(fileName, lineNum, funcName, error_class, detail)

        # print(errMsg)
        dt = datetime.now()
        write_app_log(("ERROR! %s\n + %s") % (dt.strftime('%Y-%m-%d %H:%M:%S'), errMsg))


def kill():
    pid = get_pid()
    os.popen('kill '+ pid).read().strip()

def job_nginx_main(start_time, end_time):
    db = Database()
    start_time_utc = start_time - timedelta(hours=8)
    end_time_utc = end_time - timedelta(hours=8)

    if (end_time_utc.hour != start_time_utc.hour):
        end_time_utc.replace(minute=0, second=0, microsecond=0)

    current_web_list = db.get_current_hour_web_record(start_time.strftime('%Y%m'), start_time.date(), start_time.hour)
    print(current_web_list)

    conf = configparser.ConfigParser()
    conf.read('conf.ini')
    if conf['app']['force_input_ol_domains'] == 'True':
        # force input OL data for testing
        cdn_domains = ["leacloud.net", "qnvtang.com", "leacloud.com", "reachvpn.com", "jetstartech.com", "wqlyjy.cn", "lea.cloud", "jtechcloud.com", "leaidc.com", "ahskzs.cn", "tjwohuite.com", "xcpt.cc", "qingzhuinfo.com", "www.ttt.com", "hbajhw.com", "tjflsk.com", "yeemasheji.com", "yunxingshell.com", "foxlora.com", "taoyaoyimei.com", "wlxzn.com", "anjuxinxi.com", "yueyamusic.com", "test.leacloud.net", "tongxueqn.com", "amazingthor.com", "*.wangfenghao.com", "*.xuridong10.com", "*.unnychina.com", "*.hongqiangfood.com", "*.wencangta.com", "*.zkshangcheng.com", "*.atpython.com", "*.qianchenwenwan.com", "*.51linger.com", "*.zjclwl.com", "*.daguosz.com", "*.cfbaoche.com", "*.baliangxian.com", "*.wuhanzl.com", "*.yunyishihu.com", "*.clwdfhw.com", "*.hnstrcyj.com", "*.diyaocc.com", "www.cixiweike.com", "www.lanshengyoupin.com", "www.shanlilaxian.com", "www.52duoshou.com", "www.duanjiekuai.com", "www.rbwrou.com", "www.gromgaz.com", "www.buysedo.com", "www.7723game.com", "www.360applet.com",
                       "www.huishengjin.com", "www.kanshigaoyao.com", "www.ccssjygs.com", "www.shenzhouxiaoqu.com", "www.chinaynt.com", "www.sports518.com", "www.pcgame198.com", "www.dangle789.com", "www.btsy555.com", "www.baifenyx.com", "www.pintusx.com", "www.frzhibo.com", "www.nanjingcaishui.com", "www.shsxmygs.com", "www.jsonencode.com", "www.xgmgnz.com", "www.sinceidc.com", "www.njyymzp.com", "www.bcruanlianjie.com", "www.wdmuchang.com", "www.ient2fans.com", "www.rikimrobot.com", "www.meiqiyingyu.com", "www.chengqiankj.com", "www.mifeiwangluo.com", "www.lvqqtt.com", "www.wuhanbsz.com", "www.xueqiusj.com", "www.ydllpx.com", "www.queqiaocloud.com", "www.jiajiaoshiting.com", "www.laotsai.com", "www.daoliuliang365.com", "www.jnianji.com", "www.dazhougongjiao.com", "www.hndingkun.com", "www.liangct.com", "www.amandacasa.com", "www.liwushang.com", "www.ruiyoushouyou.com", "www.yychaoli.com", "www.allcureglobal.com", "www.whrenatj.com", "yidaaaa.com", "lea.hncgw.cn", "webld.cqgame.games",
                       "test12345.tongxueqn.com"]
        not_cdn_domains = ["gotolcd.net", "adminlcd.net", "highlcd.net", "leaidc.net"]
    else:
        cdn_domains = db.get_cdn_domains()
        not_cdn_domains = db.get_not_cdn_domains()

    print(cdn_domains)
    print(not_cdn_domains)
    # all_active_domains = [x for x in cdn_domains] + [x for x in not_cdn_domains]

    period = [start_time_utc.strftime('%Y-%m-%dT%H:%M:%S'), end_time_utc.strftime('%Y-%m-%dT%H:%M:%S')]
    print(period)
    # period = ['2020-04-23T00:00:00', '2020-04-23T00:05:00']

    elastic = Elasticsearch()
    update_list = elastic.search_sendbtye_by_domains(period=period)

    for query_domain, val in update_list.items():
        domain = determin_domain(query_domain, cdn_domains, not_cdn_domains)
        print(query_domain, "determin domain is: ", domain)
        if(domain):
            write_app_log('%s => %s\n' % (query_domain, domain))
            if (domain in current_web_list):
                id = current_web_list[domain]
                db.update_web_record(start_time.strftime('%Y%m'), str(int(val[1])), str(val[0]), id)
                write_app_log('%s update: %s[%s] with sendbyte: %s count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), domain, id, str(int(val[1])), str(val[0])))
                print("update", domain, val)
            else:
                id = db.insert_web_record(start_time.strftime('%Y%m'), domain, start_time.strftime('%Y-%m-%d'), start_time.hour, str(int(val[1])), str(val[0]))
                current_web_list[domain] = id
                write_app_log('%s insert: %s[%s] with sendbyte: %s count: %s \n' % (start_time.strftime('%Y-%m-%d %H:%M:%S'), domain, id, str(int(val[1])), str(val[0])))
                print("insert", domain, val)
        else:
            write_app_log('%s is not registered in database.\n' % (query_domain))

    db.close()

def job_nginx_side(start_time, end_time):
    # start_time = datetime(2020, 4, 23, 18)
    # end_time = ''
    db = Database()
    db.update_web_bandwidth(start_time.strftime('%Y%m'), end_time.strftime('%Y-%m-%d'), end_time.hour)
    write_app_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' bandwidth updated\n')

    els = Elasticsearch()
    start_time_utc = start_time - timedelta(hours=8)
    end_time_utc = end_time - timedelta(hours=8)
    period = [start_time_utc.strftime('%Y-%m-%dT%H:%M:%S'), end_time_utc.strftime('%Y-%m-%dT%H:%M:%S')]

    # -- city count --
    dis_data = els.search_city_count_distribution(period)
    # print(dis_data)
    # exit()
    query_val = ''
    for domain, country_data in dis_data.items():
        for country, city_data in country_data.items():
            for city, count in city_data.items():
                query_val += '("%s","%s","%s","%s","%s","%s"),' % (domain, start_time.strftime('%Y-%m-%d'), start_time.hour, country, city, count)
    query_val = query_val[:-1]
    if query_val:
        db.insert_web_dist(start_time.strftime('%Y%m'), query_val)
    write_app_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' city distribution inserted\n')

    # -- status --
    elk_status_data = els.search_status_distribution(period)
    query_val = ''
    for domain, status_data in elk_status_data.items():
        for status, count in status_data.items():
            query_val += '("%s","%s","%s","%s","%s"),' % (domain, start_time.strftime('%Y-%m-%d'), start_time.hour, status, count)
    query_val = query_val[:-1]
    if query_val:
        db.insert_status_dist(start_time.strftime('%Y%m'), query_val)
    write_app_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' status distribution inserted\n')

    db.close()

def update_period(p1, p2):
    print("Start update period %s ~ %s" % (p1, p2))
    write_app_log("%s Start update period %s ~ %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), p1, p2))

    db = Database()
    period_list = []
    while p1 < p2:
        new_p1 = p1+timedelta(hours=1)
        period = (p1, new_p1)
        p1 = new_p1
        period_list.append(period)
    for period in period_list:
        db.remove_existed_data(period[0])
        job_nginx_main(period[0], period[1])
        job_nginx_side(period[0], period[1])

    write_app_log("%s Completed update period \n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

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
    else:
        eval(sys.argv[1])()