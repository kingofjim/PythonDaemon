import requests, configparser
from funcs import write_app_log

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


        body = '{"size":0,"query":{"constant_score":{"filter":{"range":{"@timestamp":{"gte":"'+period[0]+'","lt":"'+period[1]+'"}}}}},"aggs":{"domains":{"terms":{"field":"request_host.keyword","size": 999999999},"aggs":{"body_bytes_sent":{"sum":{"field":"body_bytes_sent"}}}}}}'
        write_app_log("Main - Elasticsearch: %s\n" % body)
        # exit()
        response = requests.get('http://35.201.180.3:9200/logstash-hqs-cdn-proxy-*/_search', auth=self.credentials, headers=self.headers, data=body)
        # if(response.status_code == 200):
        if(True):
            print(response.text)
            response = response.json()
            response_bucket = response['aggregations']['domains']['buckets']
            # print(response_bucket)
            if response_bucket:
                return {b['key']: [b['doc_count'],b['body_bytes_sent']['value']] for b in [x for x in response_bucket]}
            else:
                return {}
        else:
            raise Exception('Elasticsearch search_sendbtye_by_domains not respond 200\n')




    def search_city_count_distribution(self, period):
        body = '{"size":0,"query":{"constant_score":{"filter":{"range":{"@timestamp":{"gte":"'+period[0]+'","lt":"'+period[1]+'"}}}}},"aggs":{"domains":{"terms":{"field":"request_host.keyword","size": 999999999},"aggs":{"country":{"terms":{"field":"geoip.country_name.keyword"},"aggs":{"distribution":{"terms":{"size":999999999,"field":"geoip.region_name.keyword"}}}}}}}}'
        write_app_log("Side - Elasticsearch city: %s\n" % body)
        # print(body)
        # exit()
        response = requests.get('http://35.201.180.3:9200/logstash-hqs-cdn-proxy-*/_search', auth=self.credentials, headers=self.headers, data=body)
        if(response.status_code == 200):
        # if (True):
            data = {}
            response = response.json()
            # print(response)
            response_bucket = response['aggregations']['domains']['buckets']
            # print(response_bucket[0])
            for country_data in response_bucket:
                data[country_data['key']] = {}
                # print(data)
                # exit()
                for country in country_data['country']['buckets']:
                    data[country_data['key']][country['key']] = {}
                    for city in country['distribution']['buckets']:
                        data[country_data['key']][country['key']][city['key']] = city['doc_count']

            return data
        else:
            raise Exception('Elasticsearch search_city_count_distribution not respond 200\n')

    def search_status_distribution(self, period):
        body = '{"size":0,"query":{"constant_score":{"filter":{"range":{"@timestamp":{"gte":"' + period[0] + '","lt":"' + period[1] + '"}}}}},"aggs":{"domains":{"terms":{"field":"request_host.keyword","size":999999999},"aggs":{"status":{"terms":{"size":999999999,"field":"status.keyword"}}}}}}'
        write_app_log("Side - Elasticsearch status: %s\n" % body)
        # print(body)
        # exit()
        response = requests.get('http://35.201.180.3:9200/logstash-hqs-cdn-proxy-*/_search', auth=self.credentials, headers=self.headers, data=body)
        if (response.status_code == 200):
            # if (True):
            data = {}
            # print(response.text)
            response = response.json()
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
            raise Exception('Elasticsearch search_status_distribution not respond 200\n')
