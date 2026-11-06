CREATE SCHEMA mysql_shop_cdc;
CREATE TABLE mysql_shop_cdc.mysql AS
SELECT * FROM (VALUES
  ({'source': {'file': 'binlog.000002', 'pos': 198, 'gtid': NULL, 'ts_ms': 1789800000000::BIGINT},
    'ts_ms': 1789800000000::BIGINT,
    'ddl': 'DROP TABLE `customers`',
    'tableChanges': [{'type': 'DROP', 'id': '"shop"."customers"', 'table': NULL}]}),
  ({'source': {'file': 'binlog.000002', 'pos': 448, 'gtid': NULL, 'ts_ms': 1789800000001::BIGINT},
    'ts_ms': 1789800000001::BIGINT,
    'ddl': 'CREATE TABLE `customers` (`id` bigint)',
    'tableChanges': [{'type': 'CREATE', 'id': '"shop"."customers"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
  ({'source': {'file': 'binlog.000003', 'pos': 4, 'gtid': NULL, 'ts_ms': 1789900000000::BIGINT},
    'ts_ms': 1789900000000::BIGINT,
    'ddl': 'DROP TABLE `orders`',
    'tableChanges': [{'type': 'DROP', 'id': '"shop"."orders"', 'table': NULL}]})
) AS t(value)
