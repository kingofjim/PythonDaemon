import redis
import configparser


class Redis:

    def __init__(self):
        conf = configparser.ConfigParser()
        conf.read('conf.ini')
        self.r = redis.Redis(host=conf['redis']['host'], port=6379, db=0, password=conf['redis']['pass'])

    def pop(self, timeout=0):
        return self.r.blpop('cdn_logs', timeout)

    def push(self, data):
        return self.r.rpush('cdn_logs', data)