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
  ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 5::BIGINT}, 'ts_ms': 5::BIGINT,
    'ddl': 'ALTER TABLE people RENAME COLUMN name TO full_name',
    'tableChanges': [{'type': 'ALTER', 'id': '"shop"."people"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
        {'name': 'full_name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
         'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]})
) AS t(value);
CREATE SCHEMA IF NOT EXISTS cdc;
CREATE TABLE cdc."mysql.shop.people" AS SELECT * FROM (VALUES
  ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'ts_ms': 1::BIGINT},
    'before': NULL::STRUCT(id BIGINT, name VARCHAR, full_name VARCHAR),
    'after': {'id': 1::BIGINT, 'name': 'Alice', 'full_name': NULL::VARCHAR}}),
  ({'op': 'c', 'ts_ms': 2::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'ts_ms': 2::BIGINT},
    'before': NULL::STRUCT(id BIGINT, name VARCHAR, full_name VARCHAR),
    'after': {'id': 2::BIGINT, 'name': 'Bob', 'full_name': NULL::VARCHAR}}),
  ({'op': 'u', 'ts_ms': 6::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 5::BIGINT, 'ts_ms': 6::BIGINT},
    'before': {'id': 1::BIGINT, 'name': NULL::VARCHAR, 'full_name': 'Alice'},
    'after': {'id': 1::BIGINT, 'name': NULL::VARCHAR, 'full_name': 'Alicia Liddell'}})
) AS t(value)
