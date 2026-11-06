CREATE SCHEMA mysql_shop_cdc;
CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
  ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
    'ddl': 'CREATE TABLE people (id BIGINT)',
    'tableChanges': [{'type': 'CREATE', 'id': '"shop"."people"',
      'table': {'primaryKeyColumnNames': ['id'], 'columns': [
        {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
         'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
) AS t(value);
CREATE TABLE mysql_shop_cdc."mysql.shop.people" AS SELECT * FROM (VALUES
  ({'op': 'c', 'ts_ms': 1789948799100::BIGINT,
    'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789948799000::BIGINT},
    'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}}),
  ({'op': 'd', 'ts_ms': 1789948799200::BIGINT,
    'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789948798000::BIGINT},
    'before': {'id': 8::BIGINT}, 'after': NULL::STRUCT(id BIGINT)})
) AS t(value)
