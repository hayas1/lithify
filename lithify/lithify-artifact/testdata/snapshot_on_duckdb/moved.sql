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
    'ddl': 'RENAME TABLE people TO persons',
    'tableChanges': [{'type': 'ALTER', 'id': '"shop"."persons"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
        {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
         'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]})
) AS t(value);
CREATE SCHEMA IF NOT EXISTS cdc;
CREATE TABLE cdc."mysql.shop.people" AS SELECT * FROM (VALUES
  ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'ts_ms': 1::BIGINT},
    'before': NULL::STRUCT(id BIGINT, name VARCHAR), 'after': {'id': 1::BIGINT, 'name': 'Alice'}}),
  ({'op': 'c', 'ts_ms': 2::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'ts_ms': 2::BIGINT},
    'before': NULL::STRUCT(id BIGINT, name VARCHAR), 'after': {'id': 2::BIGINT, 'name': 'Bob'}})
) AS t(value);
CREATE TABLE cdc."mysql.shop.persons" AS SELECT * FROM (VALUES
  ({'op': 'u', 'ts_ms': 6::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 5::BIGINT, 'ts_ms': 6::BIGINT},
    'before': {'id': 1::BIGINT, 'name': 'Alice'}, 'after': {'id': 1::BIGINT, 'name': 'Alicia'}}),
  ({'op': 'c', 'ts_ms': 7::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 6::BIGINT, 'ts_ms': 7::BIGINT},
    'before': NULL::STRUCT(id BIGINT, name VARCHAR), 'after': {'id': 3::BIGINT, 'name': 'Carol'}})
) AS t(value)
