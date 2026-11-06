CREATE SCHEMA mysql_shop_cdc;
CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
  ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
    'ddl': 'CREATE TABLE people (id BIGINT)',
    'tableChanges': [{'type': 'CREATE', 'id': '"shop"."people"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
  ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
    'ddl': 'CREATE TABLE pets (id BIGINT)',
    'tableChanges': [{'type': 'CREATE', 'id': '"shop"."pets"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
) AS t(value);
CREATE TABLE mysql_shop_cdc."mysql.shop.people" AS SELECT * FROM (VALUES
  ({'op': 'r', 'ts_ms': 1789862400001::BIGINT,
    'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
    'before': NULL::STRUCT(id BIGINT), 'after': {'id': 1::BIGINT}}),
  ({'op': 'r', 'ts_ms': 1789862400001::BIGINT,
    'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
    'before': NULL::STRUCT(id BIGINT), 'after': {'id': 2::BIGINT}})
) AS t(value)
