import json, string, sys
from random import randint, choice


class hive_table_profile_data_generator_v2():
    def __init__(self,
                 hive_table_name,
                 hive_int_columns,
                 hive_string_columns,
                 hive_date_columns,
                 dist_val_count):

        self._entity_ = {}
        self._referenced_entities_ = {}
        self._column_guids_list_ = list()
        self._serde_list = list()
        self._hive_table_guid_ = self.get_random_guid()
        self._hive_db_guid_ = self.get_random_guid()
        self._hive_serde_guid_ = self.get_random_guid()

        self.hive_table_name = hive_table_name
        self.hive_int_columns = hive_int_columns
        self.hive_string_columns = hive_string_columns
        self.hive_date_columns = hive_date_columns
        self.dist_val_count = dist_val_count
        self.table_file_name = 'json_data/' + "hive_table_v2_with_" + str(hive_int_columns) + "intCols_" + \
                          str(hive_string_columns) + "strCols_" + \
                          str(hive_date_columns) + "dateCols_" + \
                          str(dist_val_count) + "distCount.json"
        self.hive_db = 'default'
        self.hive_cluster = 'cl1'
        self.columns = list()

    def constructHiveTableDef(self):
        payload = {}
        self.constructReferencedEntities()
        self.constructHiveTableEntity()
        payload['entity'] = self._entity_
        payload['referredEntities'] = self._referenced_entities_
        #print json.dumps(payload)
        with open(self.table_file_name, 'w') as table_file:
            table_file.write(json.dumps(payload))
        return self.table_file_name, self.hive_table_name

    def constructHiveTableEntity(self):
        '''
        {
        "attributes": {
            "aliases": null,
            "columns": [
                {
                    "guid": "-38802974-ee9f-4c18-98dd-79cb056e1587",
                    "typeName": "hive_column"
                }
            ],
            "comment": null,
            "createTime": 1502866779000,
            "db": {
                "guid": "-f4a08ff9-8cb6-4350-bb9e-38258a7074bb",
                "typeName": "hive_db"
            },
            "description": null,
            "lastAccessTime": 1502866779000,
            "name": "atlas_regression_gpteqliwxn",
            "owner": "hrt_qa",
            "parameters": {
                "COLUMN_STATS_ACCURATE": "{\"BASIC_STATS\":\"true\"}",
                "numFiles": "0",
                "numRows": "0",
                "rawDataSize": "0",
                "totalSize": "0",
                "transient_lastDdlTime": "1502866779"
            },
            "partitionKeys": null,
            "profileData": {
                "attributes": {
                    "rowCount": 10000000
                },
                "typeName": "hive_table_profile_data"
            },
            "qualifiedName": "default.atlas_regression_gpteqliwxn@cl1",
            "retention": 0,
            "sd": {
                "guid": "-e1121280-4a68-479b-be78-ec19a9e5bff7",
                "typeName": "hive_storagedesc"
            },
            "tableType": "MANAGED_TABLE",
            "temporary": false,
            "viewExpandedText": null,
            "viewOriginalText": null
        },
        "classifications": [],
        "createTime": 1502866783382,
        "createdBy": "hrt_qa",
        "guid": "-6b2d1a2b-d0fa-43d6-803a-0114abdf0a15",
        "status": "ACTIVE",
        "typeName": "hive_table",
        "updateTime": 1502866783382,
        "updatedBy": "hrt_qa",
        "version": 0
        },
        '''
        local_copy = {}
        local_copy["classifications"] = list()
        local_copy["createTime"] = 1502866783382
        local_copy["createdBy"] = "admin"
        local_copy['guid'] = self._hive_table_guid_
        local_copy['status'] = 'ACTIVE'
        local_copy['typeName'] = "hive_table"
        local_copy['updateTime'] = 1502866783382
        local_copy['updatedBy'] = "admin"
        local_copy['version'] = 0

        local_copy['attributes'] = {}
        local_copy['attributes']['aliases'] = None
        local_copy['attributes']['comment'] = None
        local_copy['attributes']['createTime'] = 1502866779000
        local_copy['attributes']['description'] = None
        local_copy['attributes']['lastAccessTime'] = 1502866779000
        local_copy['attributes']['owner'] = "admin"
        local_copy['attributes']['partitionKeys'] = None
        local_copy['attributes']['retention'] = 0
        local_copy['attributes']['tableType'] = "MANAGED_TABLE"
        local_copy['attributes']['temporary'] = False
        local_copy['attributes']['viewExpandedText'] = None
        local_copy['attributes']['viewOriginalText'] = None

        local_copy['attributes']['name'] = self.hive_table_name
        local_copy['attributes']['qualifiedName'] = self.constructTableQN()
        local_copy['attributes']['profileData'] = {}
        local_copy['attributes']['profileData']['attributes'] = {}
        local_copy['attributes']['profileData']['attributes']['rowCount'] = randint(1, sys.maxint)
        local_copy['attributes']['profileData']['typeName'] = 'hive_table_profile_data'
        local_copy['attributes']['parameters'] = {}
        local_copy['attributes']['parameters']['COLUMN_STATS_ACCURATE'] = '{\"BASIC_STATS\":\"true\"}'
        local_copy['attributes']['parameters']['numFiles'] = 0
        local_copy['attributes']['parameters']['numRows'] = 0
        local_copy['attributes']['parameters']['rawDataSize'] = 0
        local_copy['attributes']['parameters']['totalSize'] = 0
        local_copy['attributes']['parameters']['transient_lastDdlTime'] = 1502866779

        local_copy['attributes']['columns'] = []
        for each in self._column_guids_list_:
            column = {}
            column['guid'] = each
            column["typeName"] = "hive_column"
            local_copy['attributes']['columns'].append(column)

        local_copy['attributes']['sd'] = {}
        local_copy['attributes']['sd']['guid'] = self._hive_serde_guid_
        local_copy['attributes']['sd']['typeName'] = "hive_storagedesc"

        local_copy['attributes']['db'] = {}
        local_copy['attributes']['db']['guid'] = self._hive_db_guid_
        local_copy['attributes']['db']['typeName'] = "hive_storagedesc"
        self._entity_ = local_copy


    def constructReferencedEntities(self):
        self.constructHiveColumnDef()
        self.constructHiveDBDef()
        self.constructHiveSerdeDef()

    def constructHiveDBDef(self):
        '''
                "-f4a08ff9-8cb6-4350-bb9e-38258a7074bb": {
            "attributes": {
                "clusterName": "cl1",
                "description": "Default Hive database",
                "location": "hdfs://ctr-e83-1481604818073-7340-01-000005.hwx.site:8020/apps/hive/warehouse",
                "name": "default",
                "owner": "public",
                "ownerType": "ROLE",
                "parameters": null,
                "qualifiedName": "default@cl1"
            },
            "classifications": [],
            "createTime": 1502865866396,
            "guid": "-f4a08ff9-8cb6-4350-bb9e-38258a7074bb",
            "status": "ACTIVE",
            "typeName": "hive_db",
            "updateTime": 1502883033204,
            "updatedBy": "admin",
            "version": 0
        }
        '''
        local_copy = {}
        local_copy["classifications"] = list()
        local_copy["createTime"] = 1502866783382
        local_copy["createdBy"] = "admin"
        local_copy['guid'] = self._hive_db_guid_
        local_copy['status'] = 'ACTIVE'
        local_copy['typeName'] = "hive_db"
        local_copy['updateTime'] = 1502866783382
        local_copy['updatedBy'] = "admin"
        local_copy['version'] = 0
        local_copy['attributes'] = {}
        local_copy['attributes']['description'] = 'Default Hive database'
        local_copy['attributes']['owner'] = 'admin'
        local_copy['attributes']['name'] =  self.hive_db
        local_copy['attributes']['ownerType'] = "ROLE"
        local_copy['attributes']['parameters'] = None
        local_copy['attributes']['qualifiedName'] = self.constructDBQN()
        local_copy['attributes']['clusterName'] = 'cl1'
        local_copy['attributes']['location'] = "hdfs://ctr-e83-1481604818073-7340-01-000005.hwx.site:8020/apps/hive/warehouse"
        self._referenced_entities_[self._hive_db_guid_] = local_copy

    def constructHiveSerdeDef(self):
        '''
                "-e1121280-4a68-479b-be78-ec19a9e5bff7": {
            "attributes": {
                "bucketCols": null,
                "compressed": false,
                "inputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "location": "hdfs://ctr-e134-1499953498516-100980-01-000006.hwx.site:8020/apps/hive/warehouse/atlas_regression_gpteqliwxn",
                "numBuckets": -1,
                "outputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "parameters": null,
                "qualifiedName": "default.atlas_regression_gpteqliwxn@cl1_storage",
                "serdeInfo": {
                    "attributes": {
                        "name": null,
                        "parameters": {
                            "serialization.format": "1"
                        },
                        "serializationLib": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    },
                    "typeName": "hive_serde"
                },
                "sortCols": null,
                "storedAsSubDirectories": false,
                "table": {
                    "guid": "-6b2d1a2b-d0fa-43d6-803a-0114abdf0a15",
                    "typeName": "hive_table"
                }
            },
            "classifications": [],
            "createTime": 1502866783382,
            "createdBy": "hrt_qa",
            "guid": "-e1121280-4a68-479b-be78-ec19a9e5bff7",
            "status": "ACTIVE",
            "typeName": "hive_storagedesc",
            "updateTime": 1502866783382,
            "updatedBy": "hrt_qa",
            "version": 0
        },
        '''
        local_copy = {}
        local_copy["classifications"] = list()
        local_copy["createTime"] = 1502866783382
        local_copy["createdBy"] = "admin"
        local_copy['guid'] = self._hive_serde_guid_
        local_copy['status'] = 'ACTIVE'
        local_copy['typeName'] = "hive_storagedesc"
        local_copy['updateTime'] = 1502866783382
        local_copy['updatedBy'] = "admin"
        local_copy['version'] = 0
        local_copy['attributes'] = {}
        local_copy['attributes']['bucketCols'] = None
        local_copy['attributes']['compressed'] = False
        local_copy['attributes']['inputFormat'] =  "org.apache.hadoop.mapred.TextInputFormat"
        local_copy['attributes']['numBuckets'] = -1
        local_copy['attributes']['outputFormat'] = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        local_copy['attributes']['qualifiedName'] = self.constructSerdeQN()
        local_copy['attributes']['parameters'] = None
        local_copy['attributes']['location'] = "hdfs://ctr-e134-1499953498516-100980-01-000006.hwx.site:8020/apps/hive/warehouse/" + self.hive_table_name
        local_copy['attributes']['sortCols'] = None
        local_copy['attributes']['storedAsSubDirectories'] = False
        local_copy['attributes']['table'] = {}
        local_copy['attributes']['table']['guid'] = self._hive_table_guid_
        local_copy['attributes']['table']['typeName'] = "hive_table"
        local_copy['attributes']['serdeInfo'] = {}
        local_copy['attributes']['serdeInfo']['typeName'] = "hive_serde"
        local_copy['attributes']['serdeInfo']['attributes'] = {}
        local_copy['attributes']['serdeInfo']['attributes']['name'] = None
        local_copy['attributes']['serdeInfo']['attributes']['serializationLib'] = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        local_copy['attributes']['serdeInfo']['attributes']['parameters'] = {}
        local_copy['attributes']['serdeInfo']['attributes']['parameters']['serialization.format'] = 1
        self._referenced_entities_[self._hive_serde_guid_] = local_copy

    def constructHiveColumnDef(self):
        for i in range(0, self.hive_int_columns):
            self.constructHiveColumn('int')

        for i in range(0, self.hive_string_columns):
            self.constructHiveColumn('string')

        for i in range(0, self.hive_date_columns):
            self.constructHiveColumn('date')

    def constructHiveColumn(self, type):
        '''
               "-38802974-ee9f-4c18-98dd-79cb056e1587": {
            "attributes": {
                "comment": null,
                "description": null,
                "name": "id",
                "owner": "hrt_qa",
                "position": 0,
                "profileData": {
                    "attributes": {
                        "cardinality": 85000,
                        "distributionData": "{ \"1-10000\": 1000, \"10001-20000\": 1100, \"20001-30000\": 1200, \"30001-40000\": 1300, \"40001-50000\": 1400, \"50001-60000\": 1500, \"60001-70000\": 1600,\"70001-80000\": 1700, \"80001-90000\": 1900,\"90001-100000\": 2000 }",
                        "distributionKeyOrder": [
                            "1-10000",
                            "10001-20000",
                            "20001-30000",
                            "30001-40000",
                            "40001-50000",
                            "50001-60000",
                            "60001-70000",
                            "70001-80000",
                            "80001-90000",
                            "90001-100000"
                        ],
                        "distributionType": "decile-frequency",
                        "maxValue": 85001,
                        "meanValue": 10000000,
                        "medianValue": 10000000,
                        "minValue": 1,
                        "nonNullData": 100
                    },
                    "typeName": "hive_column_profile_data"
                },
                "qualifiedName": "default.atlas_regression_gpteqliwxn.id@cl1",
                "table": {
                    "guid": "-6b2d1a2b-d0fa-43d6-803a-0114abdf0a15",
                    "typeName": "hive_table"
                },
                "type": "int"
            },
            "classifications": [],
            "createTime": 1502866783382,
            "createdBy": "hrt_qa",
            "guid": "-38802974-ee9f-4c18-98dd-79cb056e1587",
            "status": "ACTIVE",
            "typeName": "hive_column",
            "updateTime": 1502866783382,
            "updatedBy": "hrt_qa",
            "version": 0
        },
        '''
        local_copy = {}
        col_name = 'col' + str(randint(1, 1000)) + self.id_generator()
        local_copy["classifications"] = list()
        local_copy["createTime"] = 1502866783382
        local_copy["createdBy"] = "admin"
        guid = self.get_random_guid()
        local_copy['guid'] = guid
        self._column_guids_list_.append(guid)
        local_copy['status'] = 'ACTIVE'
        local_copy['typeName'] = "hive_column"
        local_copy['updateTime'] = 1502866783382
        local_copy['updatedBy'] = "admin"
        local_copy['version'] = 0
        local_copy['attributes'] = {}
        local_copy['attributes']['comment'] = 'comment'
        local_copy['attributes']['description'] = None
        local_copy['attributes']['owner'] = 'admin'
        local_copy['attributes']['position'] =  0

        local_copy['attributes']['qualifiedName'] = self.constructColumnQN( col_name )
        local_copy['attributes']['profileData'] = {}
        local_copy['attributes']['profileData']['typeName'] = 'hive_column_profile_data'
        local_copy['attributes']['profileData']['attributes'] = {}
        local_copy['attributes']['profileData']['attributes']['nonNullData'] = randint(0, 100)
        local_copy['attributes']['profileData']['attributes']['cardinality'] = randint(0, 1000000)
        local_copy['attributes']['profileData']['attributes']['meanValue'] = randint(0, 100)
        local_copy['attributes']['profileData']['attributes']['medianValue'] = randint(0, 100)
        local_copy['attributes']['profileData']['attributes']['averageLength'] = randint(1, 1000)
        local_copy['attributes']['profileData']['attributes']['maxLength'] = randint(1000, 100000)
        local_copy['attributes']['profileData']['attributes']['minValue'] = randint(1, 1000)
        local_copy['attributes']['profileData']['attributes']['maxValue'] = randint(1000, 100000)
        local_copy['attributes']['type'] = type
        local_copy['attributes']['table'] = {}
        local_copy['attributes']['table']['guid'] = self._hive_table_guid_
        local_copy['attributes']['table']['typeName'] = 'hive_table'
        values = None
        order = None
        if type == 'int':
            local_copy['attributes']['name'] = 'int_' + col_name
            values, order = self.numericDistribution(self.dist_val_count)
            local_copy['attributes']['profileData']['attributes']['distributionType'] = "decile-frequency"
        elif type == 'string':
            local_copy['attributes']['name'] = 'string_' + col_name
            values, order = self.stringDistribution(self.dist_val_count)
            local_copy['attributes']['profileData']['attributes']['distributionType'] = "count-frequency"
        elif type == 'date':
            local_copy['attributes']['name'] = 'date_' + col_name
            values, order = self.dateDistribution(self.dist_val_count)
            local_copy['attributes']['profileData']['attributes']['distributionType'] = "annual"

        local_copy['attributes']['profileData']['attributes']['distributionData'] = values
        if order is not None:
            local_copy['attributes']['profileData']['attributes']['distributionKeyOrder'] = order

        self._referenced_entities_[guid] = local_copy
        #return json.loads(json.dumps(local_copy))

    def numericDistribution(self, distCount):
        distributionData = '{'
        distributionKeyOrder = []
        start = 1
        increment = 100
        for i in range(1, distCount):
            key = str(start)+'-'+str(increment)
            distributionData += ' \"'+str(start)+'-'+str(increment)+'\": ' + str(randint(1,10000)) + ','
            start = increment + 1
            increment +=100
            distributionKeyOrder.append(key)
        distributionData = distributionData[:-1]
        distributionData += ' }'
        return distributionData, distributionKeyOrder


    def stringDistribution(self, distCount):
        distributionData = '{'
        distributionKeyOrder = []

        for i in range(1, distCount):
            key = self.id_generator()
            distributionData += ' \"'+key+'\": ' + str(randint(1,10000)) + ','
            distributionKeyOrder.append(key)
        distributionData = distributionData[:-1]
        distributionData += ' }'
        return distributionData, distributionKeyOrder

    def dateDistribution(self, distCount):
        distributionData = '{'

        for i in range(1900, 1900 + distCount):
            distributionData += '\"' + str(i) + ':count' + '\"' + ':' + str(randint(1000, 100000)) + ','
            for j in range(1, 13):
                distributionData += ' \"' + str(i) + ':' + str(j) + '\"' + ':' + str(randint(1, 1000)) + ','
        distributionData = distributionData[:-1]
        distributionData += ' }'
        # pprint(distributionData)
        return distributionData, None


    def constructTableQN(self):
        return self.hive_db + '.' + self.hive_table_name + '@' + self.hive_cluster

    def constructDBQN(self):
        return self.hive_db + '@' + self.hive_cluster

    def constructColumnQN(self, name):
        return self.hive_db + '.' + self.hive_table_name + '.' + name + '@' + self.hive_cluster

    def constructSerdeQN(self):
        return self.hive_db + '.' + self.hive_table_name + '@' + self.hive_cluster + "_storage"

    def id_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(choice(chars) for _ in range(size))


    def get_random_guid(self):
        import random
        return '-'+str(random.getrandbits(64))