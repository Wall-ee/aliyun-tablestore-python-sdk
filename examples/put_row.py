# -*- coding: utf8 -*-

from example_config import *

from tablestore import *
import time

table_name = 'OTSPutRowSimpleExample'

def create_table(client):
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_options = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)
    print ('Table has been created.')

def delete_table(client):
    client.delete_table(table_name)
    print ('Table \'%s\' has been deleted.' % table_name)

def put_row(client):
    primary_key = [('gid',1), ('uid',101)]
    attribute_columns = [('name','John'), ('mobile',15100000000), ('address','China'), ('age',20)]
    row = Row(primary_key, attribute_columns)

    # Expect not exist: put it into table only when this row is not exist.
    condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)
    consumed, return_row = client.put_row(table_name, row, condition)
    print (u'Write succeed, consume %s write cu.' % consumed.write)

    row.attribute_columns = [('name','John'), ('mobile',15100000000), ('address','China'), ('age',25)]
    condition = Condition(RowExistenceExpectation.EXPECT_EXIST, SingleColumnCondition("age", 20, ComparatorType.EQUAL))
    consumed, return_row = client.put_row(table_name, row, condition)
    print (u'Write succeed, consume %s write cu.' % consumed.write)

    row.attribute_columns = [('name','John'), ('mobile',15100000000), ('address','China'), ('age',25)]

    # 上面的age已经被修改为25了，现在我们继续期望age=20，TableStore将报错
    condition = Condition(RowExistenceExpectation.EXPECT_EXIST, SingleColumnCondition("age", 20, ComparatorType.EQUAL))
    try:
        consumed,return_row = client.put_row(table_name, row, condition)
    except OTSServiceError as e:
        print (str(e))

if __name__ == '__main__':
    client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(client)
    except:
        pass
    create_table(client)

    time.sleep(3) # wait for table ready

    put_row(client)
    delete_table(client)

