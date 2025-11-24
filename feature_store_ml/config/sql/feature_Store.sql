CREATE TABLE IF NOT EXISTS feature_store.events
(
    `timestamp`     DateTime,
    `entity_id`     String,
    `event_time`    DateTime,
    `value`         Float64,
    `attribute`     Float64,
    `event_type`    String,
    `session_id`    String,
    `label`         Int8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (entity_id, timestamp)
SETTINGS index_granularity = 8192;




INSERT INTO feature_store.events
SELECT
    now() - toIntervalSecond(number * 10) AS timestamp,
    concat('entity_', toString(number % 800)) AS entity_id,
    now() - toIntervalSecond(number * 10) AS event_time,
    randNormal(500., 100.) AS value,
    randNormal(50., 10.) AS attribute,
    ['click', 'view', 'purchase', 'scroll', 'exit'][(number % 5) + 1] AS event_type,
    concat('session_', toString(number % 500)) AS session_id,
    0 AS label
FROM numbers(85000);



INSERT INTO feature_store.events
SELECT
    now() - toIntervalSecond(number * 10) AS timestamp,
    concat('entity_', toString(800 + (number % 200))) AS entity_id,
    now() - toIntervalSecond(number * 10) AS event_time,
    if((number % 2) = 0, randUniform(2000., 5000.), randUniform(1., 10.)) AS value,
    if((number % 3) = 0, randUniform(200., 500.), randUniform(0.1, 1.)) AS attribute,
    ['click', 'view', 'purchase'][(number % 3) + 1] AS event_type,
    concat('session_', toString(number % 100)) AS session_id,
    1 AS label
FROM numbers(15000);



CREATE TABLE feature_store.results
(
    `entity_id`            String,
    `event_time`           DateTime,
    `value`                Float64,
    `value_mean`           Float64,
    `value_std`            Float64,
    `value_count`          Int32,
    `value_p95`            Float64,
    `attribute_mean`       Float64,
    `feature_timestamp`    DateTime,
    `is_anomaly`           UInt8 DEFAULT 0,
    `prediction_timestamp` DateTime,
    `prediction_score`     Float64,
    `prediction_label`     UInt8,
    `materialized_at`      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (entity_id, event_time)
SETTINGS index_granularity = 8192;


CREATE TABLE feature_store.materialized_features
(
    `entity_id`         String,
    `event_time`        DateTime,
    `value`             Float64,
    `value_mean`        Float64,
    `value_std`         Float64,
    `value_count`       Int32,
    `value_p95`         Float64,
    `attribute_mean`    Float64,
    `feature_timestamp` DateTime,
    `materialized_at`   DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (entity_id, event_time)
SETTINGS index_granularity = 8192;



DROP TABLE IF EXISTS inputs;

CREATE TABLE feature_store.inputs
(
    `timestamp` DateTime DEFAULT now(),
    `entity_id` String,
    `event_time` DateTime,
    `value` Float64,                                 -- ← вернули базовое значение
    `attribute` Float64 DEFAULT randNormal(50., 10.), -- ← на случай, если код ожидает `attribute`
    `event_type` String DEFAULT 'default',
    `session_id` String DEFAULT concat('s_', toString(rand())),
    `value_mean` Float64,
    `value_std` Float64,
    `value_count` Int32,
    `value_p95` Float64,
    `attribute_mean` Float64,
    `feature_timestamp` DateTime DEFAULT now(),
    `materialized_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (entity_id, event_time)
SETTINGS index_granularity = 8192;

INSERT INTO feature_store.inputs
SELECT
    now() - toIntervalMinute(number),
    concat('entity_', toString(number % 2000)),
    now() - toIntervalMinute(number * 2),
    if(number % 5 = 0, randNormal(5000., 1500.), randNormal(500., 100.)) AS value,
    randNormal(50., 10.) AS attribute,
    'default' AS event_type,
    concat('s_', toString(rand())) AS session_id,
    if(number % 5 = 0, randNormal(5000., 1500.), randNormal(500., 100.)) AS value_mean,
    if(number % 5 = 0, randUniform(1000., 2000.), randUniform(50., 200.)) AS value_std,
    (number % 50) + 1 AS value_count,
    if(number % 5 = 0, randUniform(2000., 4000.), randUniform(400., 600.)) AS value_p95,
    if(number % 5 = 0, randNormal(500., 100.), randNormal(50., 10.)) AS attribute_mean,
    now(),
    now()
FROM numbers(100000);
