CREATE SCHEMA IF NOT EXISTS cdc;
CREATE TABLE cdc.mysql AS SELECT * FROM (VALUES
  ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
    'ddl': 'CREATE TABLE people (id BIGINT, name VARCHAR(64))',
    'tableChanges': [{'type': 'CREATE', 'id': '"shop"."people"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
        {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
         'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]}),
  ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
    'ddl': 'CREATE TABLE pets (id BIGINT)',
    'tableChanges': [{'type': 'CREATE', 'id': '"shop"."pets"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
  ({'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
    'ddl': 'CREATE TABLE gone (id BIGINT)',
    'tableChanges': [{'type': 'CREATE', 'id': '"shop"."gone"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
  ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
    'ddl': 'DROP TABLE gone',
    'tableChanges': [{'type': 'DROP', 'id': '"shop"."gone"', 'table': NULL}]})
) AS t(value);
CREATE TABLE cdc."mysql.shop.pets" AS SELECT * FROM (VALUES
  ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 5::BIGINT, 'ts_ms': 1::BIGINT},
    'before': NULL::STRUCT(id BIGINT), 'after': {'id': 7::BIGINT}})
) AS t(value)
