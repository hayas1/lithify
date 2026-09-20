INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET cdc_s3 (
    TYPE s3,
    KEY_ID getenv('S3_ACCESS_KEY_ID'),
    SECRET getenv('S3_SECRET_ACCESS_KEY'),
    REGION getenv('S3_REGION'),
    ENDPOINT regexp_replace(getenv('S3_ENDPOINT'), '^https?://', ''),
    USE_SSL starts_with(getenv('S3_ENDPOINT'), 'https://'),
    URL_STYLE 'path'
);

CREATE OR REPLACE MACRO cdc_events(prefix, topic) AS TABLE
    SELECT *
    FROM (
        SELECT
            unnest(value),
            key AS _kafka_key,
            struct_pack(
                topic := topic,
                "partition" := CAST(regexp_extract(filename, '/(\d+)-\d+\.parquet$', 1) AS BIGINT),
                "offset" := "offset",
                "timestamp" := "timestamp"
            ) AS _kafka
        FROM read_parquet(
            's3://' || getenv('S3_BUCKET') || '/' || prefix || '/' || topic || '/*.parquet',
            filename = true
        )
    )
    ORDER BY _kafka."offset";
