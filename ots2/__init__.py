# -*- coding: utf8 -*-

__version__ = '2.2.0'
__all__ = [
    'OTSClient',

    # Data Types
    'INF_MIN',
    'INF_MAX',
    'PK_AUTO_INCR',
    'TableMeta',
    'TableOptions',
    'CapacityUnit',
    'ReservedThroughput',
    'ReservedThroughputDetails',
    'ColumnType',
    'Column',
    'Direction',
    'UpdateTableResponse',
    'DescribeTableResponse',
    'RowDataItem',
    'Condition',
    'RowItem',
    'PutRowItem',
    'UpdateRowItem',
    'DeleteRowItem',
    'MultiTableInBatchGetRowItem',
    'TableInBatchGetRowItem',
    'MultiTableInBatchGetRowResult',
    'BatchWriteRowType',
    'MultiTableInBatchWriteRowItem',
    'TableInBatchWriteRowItem',
    'MultiTableInBatchWriteRowResult',
    'BatchWriteRowResponseItem',
    'OTSClientError',
    'OTSServiceError',
    'DefaultRetryPolicy',
    'LogicalOperator',
    'ComparatorType',
    'ColumnConditionType',
    'ColumnCondition',
    'CompositeCondition',
    'RelationCondition',
    'RowExistenceExpectation',
]


from ots2.client import OTSClient

from ots2.metadata import *
from ots2.error import *
from ots2.retry import *
from ots2.const import *
