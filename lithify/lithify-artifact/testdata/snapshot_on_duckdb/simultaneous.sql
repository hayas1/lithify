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
  ({'source': {'file': 'binlog.000001', 'pos': 10::BIGINT, 'gtid': NULL, 'ts_ms': 5::BIGINT}, 'ts_ms': 5::BIGINT,
    'ddl': 'ALTER TABLE people ADD COLUMN email VARCHAR(64) DEFAULT ''none''',
    'tableChanges': [{'type': 'ALTER', 'id': '"shop"."people"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
        {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
         'defaultValueExpression': NULL, 'length': 64, 'scale': NULL},
        {'name': 'email', 'typeName': 'VARCHAR', 'position': 3, 'optional': true,
         'defaultValueExpression': 'none', 'length': 64, 'scale': NULL}]}}]})
) AS t(value);
CREATE SCHEMA IF NOT EXISTS cdc;
CREATE TABLE cdc."mysql.shop.people" AS SELECT * FROM (VALUES
  ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'ts_ms': 1::BIGINT},
    'before': NULL::STRUCT(id BIGINT, name VARCHAR, email VARCHAR),
    'after': {'id': 1::BIGINT, 'name': 'Alice', 'email': NULL::VARCHAR}}),
  ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'ts_ms': 1::BIGINT},
    'before': NULL::STRUCT(id BIGINT, name VARCHAR, email VARCHAR),
    'after': {'id': 2::BIGINT, 'name': 'Bob', 'email': NULL::VARCHAR}}),
  ({'op': 'u', 'ts_ms': 5::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 8::BIGINT, 'ts_ms': 5::BIGINT},
    'before': {'id': 2::BIGINT, 'name': 'Bob', 'email': NULL::VARCHAR},
    'after': {'id': 2::BIGINT, 'name': 'Bobby', 'email': NULL::VARCHAR}}),
  ({'op': 'u', 'ts_ms': 5::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 12::BIGINT, 'ts_ms': 5::BIGINT},
    'before': {'id': 1::BIGINT, 'name': 'Alice', 'email': 'none'},
    'after': {'id': 1::BIGINT, 'name': 'Alicia', 'email': 'alicia@example.com'}})
) AS t(value)
