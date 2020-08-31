import requests, configparser
from funcs import write_app_log, write_error_log

class Elasticsearch:

    def __init__(self):
        conf = configparser.RawConfigParser()
        conf.read('conf.ini')
        self.credentials = (conf['elasticsearch']['username'], conf['elasticsearch']['password'])
        self.headers = {"Content-Type": "application/json"}
        # self.body = {}
        # self.body['headers'] = '{"Content-Type": "application/json"}'
        # self.body['json'] = '{"track_total_hits": true}'

    def search_sendbtye_by_domains(self, period):
        # body = '{"size":0,"query":{"constant_score":{"filter":{"range":{"@timestamp":{"gte":"'+period[0]+'","lt":"'+period[1]+'"}}}}},"aggs":{"domains":{"terms":{"field":"request_host.keyword","size": 999999999},"aggs":{"body_bytes_sent":{"sum":{"field":"body_bytes_sent"}}}}}}'
        body = '{"size":0,"query":{"constant_score":{"filter":{"range":{"nginx_timestamp":{"gte":"%s","lte":"%s","format":"strict_date_optional_time"}}}}},"aggs":{"domains":{"terms":{"field":"request_host.keyword","size":999999999},"aggs":{"body_bytes_sent":{"sum":{"field":"body_bytes_sent"}}}}}}' % (period[0], period[1])
        print("[Web-Main] search_sendbtye_by_domains - body: %s" % body)
        # exit()
        response = requests.get('http://35.201.180.3:9200/logstash-hqs-cdn-proxy-*/_search', auth=self.credentials, headers=self.headers, data=body)
        if(response.status_code == 200):
            # print(response.text)
            response = response.json()
            shards = response['_shards']
            if shards['total'] != shards['successful']:
                raise Exception('ERROR!!! %s ~ %s CDN main job shards failed.\n%s' % (period[0], period[1], response))
            response_bucket = response['aggregations']['domains']['buckets']
            # print(response_bucket)
            if response_bucket:
                return {b['key']: [b['doc_count'],b['body_bytes_sent']['value']] for b in [x for x in response_bucket]}
            else:
                return {}
        else:
            raise Exception('Elasticsearch search_sendbtye_by_domains not respond 200, but %s \n%s\n' % (response.status_code, response.text))

    def search_city_count_distribution(self, period):
        # body = '{"size":0,"query":{"constant_score":{"filter":{"range":{"@timestamp":{"gte":"'+period[0]+'","lt":"'+period[1]+'"}}}}},"aggs":{"domains":{"terms":{"field":"request_host.keyword","size": 999999999},"aggs":{"country":{"terms":{"field":"geoip.country_name.keyword"},"aggs":{"distribution":{"terms":{"size":999999999,"field":"geoip.region_name.keyword"}}}}}}}}'
        body = '{"size":0,"query":{"constant_score":{"filter":{"range":{"nginx_timestamp":{"gte":"%s","lte":"%s","format":"strict_date_optional_time"}}}}},"aggs":{"domains":{"terms":{"field":"request_host.keyword","size":999999999},"aggs":{"country":{"terms":{"field":"geoip.country_name.keyword"},"aggs":{"distribution":{"terms":{"size":999999999,"field":"geoip.region_name.keyword"}}}}}}}}' % (period[0], period[1])
        write_app_log("[Web] search_city_count_distribution - body: %s\n" % body)
        response = requests.get('http://35.201.180.3:9200/logstash-hqs-cdn-proxy-*/_search', auth=self.credentials, headers=self.headers, data=body)
        if(response.status_code == 200):
            response = response.json()
            shards = response['_shards']
            if shards['total'] != shards['successful']:
                raise Exception('ERROR!!! %s ~ %s CDN main job shards failed.\n%s' % (period[0], period[1], response))
            data = {}
            response_bucket = response['aggregations']['domains']['buckets']
            for country_data in response_bucket:
                data[country_data['key']] = {}
                for country in country_data['country']['buckets']:
                    data[country_data['key']][country['key']] = {}
                    for city in country['distribution']['buckets']:
                        data[country_data['key']][country['key']][city['key']] = city['doc_count']

            return data
        else:
            raise Exception('Elasticsearch search_city_count_distribution not respond 200, but %s \n%s\n' % (response.status_code, response.text))

    def search_status_distribution(self, period):
        # body = '{"size":0,"query":{"constant_score":{"filter":{"range":{"@timestamp":{"gte":"' + period[0] + '","lt":"' + period[1] + '"}}}}},"aggs":{"domains":{"terms":{"field":"request_host.keyword","size":999999999},"aggs":{"status":{"terms":{"size":999999999,"field":"status.keyword"}}}}}}'
        body = '{"size":0,"query":{"constant_score":{"filter":{"range":{"nginx_timestamp":{"gte":"%s","lte":"%s","format":"strict_date_optional_time"}}}}},"aggs":{"domains":{"terms":{"field":"request_host.keyword","size":999999999},"aggs":{"status":{"terms":{"size":999999999,"field":"status.keyword"}}}}}}' % (period[0], period[1])
        print("[Web] - Elasticsearch status - body: %s" % body)
        # print(body)
        # exit()
        response = requests.get('http://35.201.180.3:9200/logstash-hqs-cdn-proxy-*/_search', auth=self.credentials, headers=self.headers, data=body)
        if (response.status_code == 200):
            response = response.json()
            shards = response['_shards']
            if shards['total'] != shards['successful']:
                raise Exception('ERROR!!! %s ~ %s DNS main job shards failed.\n%s' % (period[0], period[1], response))
            # if (True):
            data = {}
            # print(response.text)
            # print(response)
            # exit()
            response_bucket = response['aggregations']['domains']['buckets']
            for status_data in response_bucket:
                data[status_data['key']] = {}
                # exit()
                for status_count in status_data['status']['buckets']:
                    data[status_data['key']][status_count['key']] = status_count['doc_count']
            return data
        else:
            raise Exception('Elasticsearch search_status_distribution not respond 200, but %s \n%s\n' % (response.status_code, response.text))

    def search_dns_query_by_domains(self, period):

        body = '{"aggs":{"2":{"terms":{"field":"query_value.keyword","order":{"_count":"desc"},"size":9999999}}},"size":0,"_source":{"excludes":[]},"stored_fields":["*"],"script_fields":{},"docvalue_fields":[{"field":"@timestamp","format":"date_time"}],"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"range":{"@timestamp":{"format":"strict_date_optional_time","gte":"%s","lt":"%s"}}}],"should":[],"must_not":[]}}}' % (period[0], period[1])
        print("[DNS] search_dns_query_by_domains - body: %s" % body)
        # exit()
        response = requests.get('http://35.201.180.3:9200/logstash-hqs-cdn-dns-*/_search/', auth=self.credentials, headers=self.headers, data=body)
        if response.status_code == 200:
            # print(response.text)
            response = response.json()
            shards = response['_shards']
            if shards['total'] != shards['successful']:
                raise Exception('ERROR!!! %s ~ %s DNS main job shards failed.\n%s' % (period[0], period[1], response))
            response_bucket = response['aggregations']['2']['buckets']
            # print(response_bucket)
            if response_bucket:
                something = {x['key']: x['doc_count'] for x in response_bucket}
                return something
            else:
                return {}
        else:
            raise Exception('Elasticsearch search_dns_query_by_domains not respond 200, but %s \n%s\n' % (response.status_code, response.text))

    def search_dns_query_by_ip(self, period):
        body = '{"aggs":{"2":{"terms":{"field":"client_ip.keyword","order":{"_count":"desc"},"size":9999999,"min_doc_count":100},"aggs":{"3":{"terms":{"field":"query_value.keyword","order":{"_count":"desc"},"size":999999}}}}},"size":0,"_source":{"excludes":[]},"stored_fields":["*"],"script_fields":{},"docvalue_fields":[{"field":"@timestamp","format":"date_time"}],"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"range":{"@timestamp":{"format":"strict_date_optional_time","gte":"%s","lt":"%s"}}}],"should":[],"must_not":[]}}}' % (period[0], period[1])
        print("[DNS] search_dns_query_by_ip - body: %s" % body)
        # exit()
        response = requests.get('http://35.201.180.3:9200/logstash-hqs-cdn-dns-*/_search/', auth=self.credentials, headers=self.headers, data=body)
        if response.status_code == 200:
            # print(response.text)
            response = response.json()
            shards = response['_shards']
            if shards['total'] != shards['successful']:
                raise Exception('ERROR!!! %s ~ %s DNS-IP job shards failed.\n%s' % (period[0], period[1], response))
            response_bucket = response['aggregations']['2']['buckets']
            # print(response_bucket)

            if response_bucket:
                result = {}
                for ip_data in response_bucket:
                    ip = ip_data['key']
                    for data in ip_data['3']['buckets']:
                        domain = data['key'].lower()
                        count = data['doc_count']
                        if ip in result:
                            result[ip][domain] = count
                        else:
                            result[ip] = {domain: count}
                return result
            else:
                return {}
        else:
            raise Exception('Elasticsearch search_dns_query_by_ip not respond 200, but %s \n%s\n' % (response.status_code, response.text))