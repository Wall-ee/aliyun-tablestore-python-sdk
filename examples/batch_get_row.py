# -*- coding: utf8 -*-

from example_config import *
from ots2 import *
import time

table_name = 'OTSBatchGetRowSimpleExample'

def create_table(ots_client):
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'INTEGER')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_option = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    ots_client.create_table(table_meta, table_option, reserved_throughput)
    print 'Table has been created.'

def delete_table(ots_client):
    ots_client.delete_table(table_name)
    print 'Table \'%s\' has been deleted.' % table_name

def put_row(ots_client):
    for i in range(0, 10):
        primary_key = [('gid',i), ('uid',i+1)]
        attribute_columns = [('name','John'), ('mobile',i), ('address','China'), ('age',i)]
        condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST) # Expect not exist: put it into table only when this row is not exist.
        consumed,pk,attr = ots_client.put_row(table_name, condition, primary_key, attribute_columns)
        print u'Write succeed, consume %s write cu.' % consumed.write

def batch_get_row(ots_client):
    # try get 10 rows from exist table and 10 rows from not-exist table
    columns_to_get = ['name', 'mobile', 'address', 'age']
    rows_to_get = []
    for i in range(0, 10):
        primary_key = [('gid',i), ('uid',i+1)]
        rows_to_get.append(primary_key)

    cond = CompositeCondition(LogicalOperator.AND)
    cond.add_sub_condition(RelationCondition("name", "John", ComparatorType.EQUAL))
    cond.add_sub_condition(RelationCondition("address", 'China', ComparatorType.EQUAL))

    request = MultiTableInBatchGetRowItem()
    request.add(TableInBatchGetRowItem(table_name, rows_to_get, columns_to_get, cond, 1))
    request.add(TableInBatchGetRowItem('notExistTable', rows_to_get, columns_to_get, cond, 1))

    result = ots_client.batch_get_row(request)

    print 'Result status: %s'%(result.is_all_succeed())
    
    table_result_0 = result.get_result_by_table(table_name)
    table_result_1 = result.get_result_by_table('notExistTable')

    print 'Check first table\'s result:'     
    for item in table_result_0:
        if item.is_ok:
            print 'Read succeed, PrimaryKey: %s, Attributes: %s' % (item.primary_key_columns, item.attribute_columns)
        else:
            print 'Read failed, error code: %s, error message: %s' % (item.error_code, item.error_message)

    print 'Check second table\'s result:'
    for item in table_result_1:
        if item.is_ok:
            print 'Read succeed, PrimaryKey: %s, Attributes: %s' % (item.primary_key_columns, item.attribute_columns)
        else:
            print 'Read failed, error code: %s, error message: %s' % (item.error_code, item.error_message)

if __name__ == '__main__':
    ots_client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(ots_client)
    except:
        pass

    create_table(ots_client)

    time.sleep(3) # wait for table ready
    put_row(ots_client)
    batch_get_row(ots_client)
    delete_table(ots_client)

