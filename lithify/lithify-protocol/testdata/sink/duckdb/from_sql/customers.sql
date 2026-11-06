SELECT {
  'source': {'file': 'binlog.000002', 'pos': 448, 'gtid': NULL},
  'ts_ms': 1789800000000::BIGINT,
  'ddl': 'CREATE TABLE `customers` (`id` bigint)',
  'tableChanges': [{
    'type': 'CREATE',
    'id': '"shop"."customers"',
    'table': {
      'primaryKeyColumnNames': ['id'],
      'columns': [{
        'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
        'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL
      }, {
        'name': 'amount', 'typeName': 'DECIMAL', 'position': 2, 'optional': true,
        'defaultValueExpression': '0.00', 'length': 12, 'scale': 2
      }]
    }
  }]
}
