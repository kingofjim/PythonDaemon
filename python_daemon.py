from red import Redis
import json, configparser
from converter import Converter
from funcs import write_app_log, write_pid, get_pid
import sys, traceback, time
from database import Database
import os, re, datetime



def start():
    pid = os.getpid()
    write_pid(str(pid))

    # start = time.time()
    dt = datetime.datetime.now()
    write_app_log('Daemon Start - %s\n' % (dt.strftime('%Y-%m-%d %H:%M:%S')))

    red = Redis()
    db = Database()
    conf = configparser.ConfigParser()
    conf.read('conf.ini')
    debug = True if conf['app']['debug'] == 'True' else 0

    # count = 1
    while (True):
        try:
            # print("Start")

            # log = {
            #     "c": 3395661186,
            #     "mt": "ns",
            #     "offset": 23759119,
            #     "q": "all.gameblogger.site.hnyhzs.com",
            #     "qt": "A",
            #     "qz": "ZJ",
            #     "ts": 1586078179,
            #     "client_ip": "101.200.135.25"
            # }
            log = None
            log = json.loads(red.pop()[1])

            Converter(log, db, debug)
            # exit()

        except KeyboardInterrupt:
            sys.exit('Force Terminate Python Daemon')

        except:
            # retry
            retry = 0
            while (True):
                try:
                    retry += 1
                    time.sleep(5)
                    print('retry: ', retry)
                    if log is None:
                        red = Redis()
                        log = json.loads(red.pop()[1])

                    db = Database()
                    Converter(log, db, debug)
                    break
                except Exception as e:
                    print(e)
                    error_class = e.__class__.__name__  # 取得錯誤類型
                    detail = e.args[0]  # 取得詳細內容
                    cl, exc, tb = sys.exc_info()  # 取得Call Stack

                    errMsg = ''
                    for lastCallStack in traceback.extract_tb(tb):
                        fileName = lastCallStack[0]  # 取得發生的檔案名稱
                        lineNum = lastCallStack[1]  # 取得發生的行號
                        funcName = lastCallStack[2]  # 取得發生的函數名稱
                        errMsg += "File \"{}\", line {}, in {}: [{}] {}\n".format(fileName, lineNum, funcName, error_class, detail)

                    print(log)
                    print(errMsg)
                    dt = datetime.datetime.now()
                    write_app_log(("%s\nLog: " + str(log) + '\n' + errMsg) % (dt.strftime('%Y-%m-%d %H:%M:%S')))

        # count += 1
        # if(debug):
        #     print(count)

    # end = time.time()
    # print("Completed in: ", end - start, " seconds")

def kill():
    pid = get_pid()
    os.popen('kill '+ pid).read().strip()

# data = json.loads(red.pop())
# data = {'c': 3670817576, 'mt': 'ns', 'offset': 36782851, 'q': 'tcsh.ns.leacloud.net', 'qt': 'AAAA', 'qz': 'GX', 'ts': 1586078184, 'client_ip': '35.220.246.26'}
# log = {
#   "addr": 2936342103,
#   "byte": 4918,
#   "city": "Changsha",
#   "country": "China",
#   "mt": "nx",
#   "offset": 1073638,
#   "query": "www.hbajhw.com",
#   "status": 200,
#   "ts": 1586078189,
#   "client_ip": "120.206.184.14"
# }
# log = {
#   "c": 3395661186,
#   "mt": "ns",
#   "offset": 23759119,
#   "q": "all.gameblogger.site.hnyhzs.com",
#   "qt": "A",
#   "qz": "ZJ",
#   "ts": 1586078179,
#   "client_ip": "101.200.135.25"
# }
# log = {
#   "addr": 1971875173,
#   "byte": 68,
#   "city": "",
#   "country": "China",
#   "mt": "nx",
#   "offset": 78671,
#   "query": "www.daguosz.com",
#   "status": 200,
#   "ts": 1586078193,
#   "client_ip": "61.174.253.41"
# }

# log = {
#   "mt": "touch",
#   "client_ip": "129.204.238.216"
# }

# log = {
#   "mt": "sysinfo",
#   "info": {
#     "type": "inet",
#     "inet": "eth0 ",
#     "TX": "89536843789",
#     "RX": "97812913015",
#     "v": "1.9.6"
#   },
#   "client_ip": "129.204.238.216"
# }
#

# log = {'mt': 'nxc', 'st': 'ok', 'client_ip': '129.204.238.216'}
# log = {'mt': 'nxr', 'reload': 'ok', 'client_ip': '129.204.238.216'}
#
# con = Converter(log)
# print(sys.argv)
# print(1 in sys.argv)
if(len(sys.argv) > 1):
    eval(sys.argv[1])()