# -*- coding: utf8 -*-

from example_config import *
from ots2 import *
import time

table_name = 'OTSTableOperationsSimpleExample'

def create_table(ots_client):
    schema_of_primary_key = [('gid', 'INTEGER'), ('uid', 'STRING')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_option = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    ots_client.create_table(table_meta, table_option, reserved_throughput)
    print ('Table has been created.')

def list_table(ots_client):
    tables = ots_client.list_table()
    print ('All the tables you have created:')
    for table in tables:
        print (table)

def describe_table(ots_client):
    describe_response = ots_client.describe_table(table_name)
    print ('TableName: %s' % describe_response.table_meta.table_name)
    print ('PrimaryKey: %s' % describe_response.table_meta.schema_of_primary_key)
    print ('Reserved read throughput: %s' % describe_response.reserved_throughput_details.capacity_unit.read)
    print ('Reserved write throughput: %s' % describe_response.reserved_throughput_details.capacity_unit.write)
    print ('Last increase throughput time: %s' % describe_response.reserved_throughput_details.last_increase_time)
    print ('Last decrease throughput time: %s' % describe_response.reserved_throughput_details.last_decrease_time)
    print ('table options\'s time to live: %s' % describe_response.table_options.time_to_live)
    print ('table options\'s max version: %s' % describe_response.table_options.max_version)
    print ('table options\'s max_time_deviation: %s' % describe_response.table_options.max_time_deviation)

        
def update_table(ots_client):
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    table_option = TableOptions(100000, 2)
    update_response = ots_client.update_table(table_name, table_option, reserved_throughput)
    print ('Reserved read throughput: %s' % update_response.reserved_throughput_details.capacity_unit.read)
    print ('Reserved write throughput: %s' % update_response.reserved_throughput_details.capacity_unit.write)
    print ('Last increase throughput time: %s' % update_response.reserved_throughput_details.last_increase_time)
    print ('Last decrease throughput time: %s' % update_response.reserved_throughput_details.last_decrease_time)
    print ('table options\'s time to live: %s' % update_response.table_options.time_to_live)
    print ('table options\'s max version: %s' % update_response.table_options.max_version)
    print ('table options\'s max_time_deviation: %s' % update_response.table_options.max_time_deviation)

def delete_table(ots_client):
    ots_client.delete_table(table_name)
    print ('Table \'%s\' has been deleted.' % table_name)

if __name__ == '__main__':
    ots_client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)
    try:
        delete_table(ots_client)
    except:
        pass
    create_table(ots_client)

    time.sleep(3) # wait for table ready

    list_table(ots_client)
    describe_table(ots_client)
    update_table(ots_client)
    #delete_table(ots_client)

