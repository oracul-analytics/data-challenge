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
    event_time        DateTime,
    entity_id         String,
    created_at        DateTime,
    value             Float64,
    attribute         Float64,
    event_type        String,
    session_id        String,
    value_mean        Float64,
    value_std         Float64,
    value_count       UInt32,
    value_p95         Float64,
    attribute_mean    Float64,
    updated_at        DateTime,
    inserted_at       DateTime
)
ENGINE = MergeTree()
ORDER BY (entity_id, event_time);

DROP TABLE IF EXISTS feature_store.inputs;

CREATE TABLE feature_store.inputs
(
    timestamp         DateTime DEFAULT now(),
    entity_id         String,
    event_time        DateTime,
    value             Float64,
    attribute         Float64,
    event_type        String,
    session_id        String,
    value_mean        Float64,
    value_std         Float64,
    value_count       UInt32,
    value_p95         Float64,
    attribute_mean    Float64,
    feature_timestamp DateTime DEFAULT now(),
    materialized_at   DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (entity_id, event_time)
SETTINGS index_granularity = 8192;

INSERT INTO feature_store.inputs
SELECT 
    now() - toIntervalMinute(number) AS timestamp,
    concat('entity_', toString(number % 1600)) AS entity_id,
    now() - toIntervalMinute(number * 2) AS event_time,
    randNormal(500., 100.) AS value,
    randNormal(50., 10.) AS attribute,
    'normal' AS event_type,
    concat('s_', toString(rand())) AS session_id,
    randNormal(480., 90.) AS value_mean,
    randUniform(50., 150.) AS value_std,
    (number % 30) + 10 AS value_count,
    randUniform(550., 650.) AS value_p95,
    randNormal(48., 9.) AS attribute_mean,
    now() AS feature_timestamp,
    now() AS materialized_at
FROM numbers(80000);

INSERT INTO feature_store.inputs
SELECT 
    now() - toIntervalMinute(number) AS timestamp,
    concat('entity_', toString(1600 + number % 400)) AS entity_id,
    now() - toIntervalMinute(number * 2) AS event_time,
    if((number % 2) = 0, 
       randUniform(3000., 6000.),
       randUniform(1., 50.)
    ) AS value,
    if((number % 3) = 0, 
       randUniform(200., 600.), 
       randUniform(0.1, 2.)
    ) AS attribute,
    'artifact' AS event_type,
    concat('s_', toString(rand())) AS session_id,
    randNormal(500., 100.) AS value_mean,
    randUniform(300., 800.) AS value_std,
    (number % 10) + 1 AS value_count,
    randUniform(2000., 5000.) AS value_p95,
    if((number % 3) = 0, 
       randNormal(300., 80.), 
       randNormal(1., 0.5)
    ) AS attribute_mean,
    now() AS feature_timestamp,
    now() AS materialized_at
FROM numbers(20000);
