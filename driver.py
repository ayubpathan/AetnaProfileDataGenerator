import requests
from requests.auth import HTTPBasicAuth
import json
from GenerateFullHiveTable import hive_table_profile_data_generator
from GenerateFullHiveTableV2 import hive_table_profile_data_generator_v2
from testData import TestData


def http_request(url, body, method):
    response_body = None
    status = None
    header = {'Content-Type': 'application/json'}
    try:
        basic_auth = HTTPBasicAuth('admin', 'admin')
        response = requests.request(method=method,
                                    url=url,
                                    auth=basic_auth,
                                    headers=header,
                                    verify=False,
                                    json=body)
        status = response.status_code
        print "status : " + str(status)
        assert ( status == 201 or status == 200 ), "entity creation failed"
        if status != 204:
            response_body = response.json()
    except requests.exceptions.RequestException as e:
        print ("exception in http request : " + str(e))
    return response_body, status


def post(host, input_json_file, v2=False):
    url = 'http://'+host+':21000/api/atlas/entities'
    json_data = None
    if input_json_file:
        json_data = json.loads(open(input_json_file).read())
    if v2:
        url = 'http://' + host + ':21000/api/atlas/v2/entity'
    http_request(url, json_data, 'POST')


if __name__ == "__main__":
    import sys
    atlas_host = sys.argv[1]
    no_of_entities = int(sys.argv[2])
    is_v2 = True if sys.argv[3].lower() == 'true' else False
    for test in TestData.generate_input_data(no_of_entities):
        if is_v2:
            data_gen = hive_table_profile_data_generator_v2(test[0], test[1], test[2], test[3], test[4])
        else:
            data_gen = hive_table_profile_data_generator(test[0], test[1], test[2], test[3], test[4])
        filename, table_name = data_gen.constructHiveTableDef()
        print "Hostname: " + host + " trying to create table with name: " + str(table_name) +" and file name is: " + str(filename)
        post(atlas_host, filename, is_v2)

