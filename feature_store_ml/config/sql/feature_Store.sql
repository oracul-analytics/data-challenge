CREATE TABLE IF NOT EXISTS feature_store.events
(
    `timestamp` DateTime,
    `entity_id` String,
    `event_time` DateTime,
    `value` Float64,
    `attribute` Float64,
    `event_type` String,
    `session_id` String,
    `label` Int8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (entity_id, timestamp)
SETTINGS index_granularity = 8192




SELECT
    now() - toIntervalSecond(number * 10) AS timestamp,
    concat('entity_', toString(number % 800)) AS entity_id,
    now() - toIntervalSecond(number * 10) AS event_time,
    randNormal(500.0, 100.0) AS value,
    randNormal(50.0, 10.0) AS attribute,
    arrayElement(['click', 'view', 'purchase', 'scroll', 'exit'], (number % 5) + 1) AS event_type,
    concat('session_', toString(number % 500)) AS session_id,
    0 AS label
FROM numbers(85000);

INSERT INTO feature_store.events SELECT
    now() - toIntervalSecond(number * 10) AS timestamp,
    concat('entity_', toString(number % 800)) AS entity_id,
    now() - toIntervalSecond(number * 10) AS event_time,
    randNormal(500., 100.) AS value,
    randNormal(50., 10.) AS attribute,
    ['click', 'view', 'purchase', 'scroll', 'exit'][(number % 5) + 1] AS event_type,
    concat('session_', toString(number % 500)) AS session_id,
    0 AS label
FROM numbers(85000)



SELECT
    now() - toIntervalSecond(number * 10) AS timestamp,
    concat('entity_', toString(800 + (number % 200))) AS entity_id,
    now() - toIntervalSecond(number * 10) AS event_time,
    if(number % 2 = 0, randUniform(2000.0, 5000.0), randUniform(1.0, 10.0)) AS value,
    if(number % 3 = 0, randUniform(200.0, 500.0), randUniform(0.1, 1.0)) AS attribute,
    arrayElement(['click', 'view', 'purchase'], (number % 3) + 1) AS event_type,
    concat('session_', toString(number % 100)) AS session_id,
    1 AS label
FROM numbers(15000);

INSERT INTO feature_store.events SELECT
    now() - toIntervalSecond(number * 10) AS timestamp,
    concat('entity_', toString(800 + (number % 200))) AS entity_id,
    now() - toIntervalSecond(number * 10) AS event_time,
    if((number % 2) = 0, randUniform(2000., 5000.), randUniform(1., 10.)) AS value,
    if((number % 3) = 0, randUniform(200., 500.), randUniform(0.1, 1.)) AS attribute,
    ['click', 'view', 'purchase'][(number % 3) + 1] AS event_type,
    concat('session_', toString(number % 100)) AS session_id,
    1 AS label
FROM numbers(15000)





CREATE TABLE IF NOT EXISTS feature_store.production_events
(
    `timestamp` DateTime,
    `entity_id` String,
    `event_time` DateTime,
    `value` Float64,
    `attribute` Float64,
    `event_type` String,
    `session_id` String,
    INDEX idx_entity_id entity_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_event_type event_type TYPE bloom_filter GRANULARITY 1,
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (entity_id, timestamp)
TTL timestamp + toIntervalDay(90)
SETTINGS index_granularity = 8192





INSERT INTO feature_store.production_events SELECT
    timestamp,
    entity_id,
    event_time,
    value,
    attribute,
    event_type,
    session_id
FROM feature_store.events
WHERE timestamp >= (now() - toIntervalHour(24))
LIMIT 1000




CREATE TABLE feature_store.results
(
    `entity_id` String,
    `event_time` DateTime,
    `value` Float64,
    `value_mean` Float64,
    `value_std` Float64,
    `value_count` Int32,
    `value_p95` Float64,
    `attribute_mean` Float64,
    `feature_timestamp` DateTime
)
ENGINE = MergeTree
ORDER BY (entity_id, event_time)
