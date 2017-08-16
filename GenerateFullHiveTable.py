import json, string, sys
from random import randint, choice

class hive_table_profile_data_generator():
    def __init__(self,
                 hive_table_name,
                 hive_int_columns,
                 hive_string_columns,
                 hive_date_columns,
                 dist_val_count):
        with open('hive_table.json') as data_file:
            data = json.load(data_file)

        self._hdb_def_ = data[0]
        self._htable_def_ = data[1]

        self.hive_table_name = hive_table_name
        self.hive_int_columns = hive_int_columns
        self.hive_string_columns = hive_string_columns
        self.hive_date_columns = hive_date_columns
        self.dist_val_count = dist_val_count
        '''
        hive_table_name = sys.argv[1]
        hive_int_columns = sys.argv[2]
        hive_string_columns = sys.argv[3]
        hive_date_columns = sys.argv[4]
        dist_val_count = int(sys.argv[5])

        hive_table_name = 'dataprofile_testtable' + str(randint(1,1000)) + id_generator()
        hive_int_columns = int(sys.argv[1])
        hive_string_columns = int(sys.argv[2])
        hive_date_columns = int(sys.argv[3])
        dist_val_count = int(sys.argv[4])
        '''
        self.table_file_name = 'json_data/' + "hive_table_with_" + str(hive_int_columns) + "intCols_" + \
                          str(hive_string_columns) + "strCols_" + \
                          str(hive_date_columns) + "dateCols_" + \
                          str(dist_val_count) + "distCount.json"
        self.hive_db = 'default'
        self.hive_cluster = 'cl1'
        self.columns = list()

    def id_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(choice(chars) for _ in range(size))

    def constructHiveTableDef(self):
        local_copy_htable = self._htable_def_
        local_copy_hdb = self._hdb_def_
        payload = []
        payload.append(local_copy_hdb)
        local_copy_htable['values']['name'] = self.hive_table_name
        #local_copy_htable['values']['createTime'] =
        local_copy_htable['values']['qualifiedName'] = self.constructTableQN()
        local_copy_htable['values']['profileData']['values']['rowCount'] = randint(1, sys.maxint)
        local_copy_htable['values']['parameters']['COLUMN_STATS_ACCURATE'] = "true"
        column_samples = local_copy_htable['values']['columns']
        columns = self.constructHiveColumnDef(column_samples, self.dist_val_count)
        local_copy_htable['values']['columns'] = json.loads(json.dumps(columns))
        local_copy_htable['values']['sd']['values']['location'] = 'hdfs://hostname/apps/hive/warehouse/'+self.hive_table_name
        local_copy_htable['values']['sd']['values']['qualifiedName'] = self.constructTableQN() + '_storage'
        payload.append(local_copy_htable)
        #print json.dumps(payload)
        with open(self.table_file_name, 'w') as table_file:
            table_file.write(json.dumps(payload))
        return self.table_file_name, self.hive_table_name


    def constructTableQN(self):
        return self.hive_db + '.' + self.hive_table_name + '@' + self.hive_cluster


    def constructColumnQN(self, name):
        return self.hive_db + '.' + self.hive_table_name + '.' + name + '@' + self.hive_cluster


    def constructHiveColumnDef(self, column_samples, count):
        for i in range(0, self.hive_int_columns):
            column = self.constructHiveColumn(column_samples[0], count)
            self.columns.append(column)

        for i in range(0, self.hive_string_columns):
            column = self.constructHiveColumn(column_samples[1], count)
            self.columns.append(column)

        for i in range(0, self.hive_date_columns):
            column = self.constructHiveColumn(column_samples[2], count)
            self.columns.append(column)
        return self.columns


    def constructHiveColumn(self, column_sample, distCount=100):
        local_copy = column_sample.copy()
        col_name = 'col' + str(randint(1, 1000)) + self.id_generator()
        local_copy['id']['id'] = "-"+str(randint(10000, 99999))+str(randint(10000, 99999))+str(randint(10000, 99999))

        local_copy['values']['qualifiedName'] = self.constructColumnQN( col_name )
        local_copy['values']['profileData']['values']['nonNullData'] = randint(0, 100)
        local_copy['values']['profileData']['values']['cardinality'] = randint(0, 1000000)
        local_copy['values']['profileData']['values']['meanValue'] = randint(0, 100)
        local_copy['values']['profileData']['values']['medianValue'] = randint(0, 100)
        local_copy['values']['profileData']['values']['averageLength'] = randint(1, 1000)
        local_copy['values']['profileData']['values']['maxLength'] = randint(1000, 100000)
        local_copy['values']['profileData']['values']['minValue'] = randint(1, 1000)
        local_copy['values']['profileData']['values']['maxValue'] = randint(1000, 100000)
        if local_copy['values']['type'] == 'int':
            local_copy['values']['name'] = 'int_' + col_name
            values, order = self.numericDistribution(distCount)
        elif local_copy['values']['type'] == 'string':
            local_copy['values']['name'] = 'string_' + col_name
            values, order = self.stringDistribution(distCount)
        elif local_copy['values']['type'] == 'date':
            local_copy['values']['name'] = 'date_' + col_name
            values, order = self.dateDistribution(distCount)

        local_copy['values']['profileData']['values']['distributionData'] = values
        if order is not None:
            local_copy['values']['profileData']['values']['distributionKeyOrder'] = order
        return json.loads(json.dumps(local_copy))

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