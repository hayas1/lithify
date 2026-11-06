CREATE SCHEMA IF NOT EXISTS cdc;
CREATE TABLE cdc."mysql.shop.people" AS SELECT * FROM (VALUES
  ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'ts_ms': 1::BIGINT},
    'before': NULL, 'after': {'id': 1::BIGINT, 'name': 'Alice'}}),
  ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'ts_ms': 1::BIGINT},
    'before': NULL, 'after': {'id': 2::BIGINT, 'name': 'Bob'}}),
  ({'op': 'u', 'ts_ms': 2::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'ts_ms': 2::BIGINT},
    'before': {'id': 1::BIGINT, 'name': 'Alice'}, 'after': {'id': 1::BIGINT, 'name': 'Alicia'}}),
  ({'op': 'd', 'ts_ms': 3::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'ts_ms': 3::BIGINT},
    'before': {'id': 2::BIGINT, 'name': 'Bob'}, 'after': NULL})
) AS t(value)
