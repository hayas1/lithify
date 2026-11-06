CREATE SCHEMA IF NOT EXISTS cdc;
CREATE TABLE cdc.mysql AS SELECT * FROM (VALUES
  ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
    'ddl': 'CREATE TABLE people (id BIGINT)',
    'tableChanges': [{'type': 'CREATE', 'id': '"shop"."people"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
  ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 5::BIGINT}, 'ts_ms': 5::BIGINT,
    'ddl': 'ALTER TABLE people ADD COLUMN seen_at DATETIME DEFAULT CURRENT_TIMESTAMP',
    'tableChanges': [{'type': 'ALTER', 'id': '"shop"."people"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
        {'name': 'seen_at', 'typeName': 'DATETIME', 'position': 2, 'optional': true,
         'defaultValueExpression': 'CURRENT_TIMESTAMP', 'length': NULL, 'scale': NULL}]}}]})
) AS t(value);
CREATE SCHEMA IF NOT EXISTS cdc;
CREATE TABLE cdc."mysql.shop.people" AS SELECT * FROM (VALUES
  ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'ts_ms': 1::BIGINT},
    'before': NULL::STRUCT(id BIGINT, seen_at BIGINT), 'after': {'id': 1::BIGINT, 'seen_at': NULL::BIGINT}})
) AS t(value)
