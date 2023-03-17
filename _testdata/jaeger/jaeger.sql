-- noinspection SqlNoDataSourceInspectionForFile

-- sample CREATE TABLE trino statement
CREATE TABLE chronowave.system.jaeger_test
(
    trace_id       VARCHAR,
    span_id        VARCHAR,
    operation_name VARCHAR,
    references     ARRAY( ROW (
        trace_id VARCHAR,
        span_id VARCHAR,
        ref_type INTEGER )),
    flags          INTEGER,
    start_time     TIMESTAMP WITH (layout = '2006-01-02T15:04:05.999999999Z07:00'),
    duration       BIGINT,
    tags ARRAY ( ROW (
        key VARCHAR,
        v_type INTEGER,
        v_str VARCHAR,
        v_bool BOOLEAN,
        v_int64 BIGINT,
        v_float64 DOUBLE )),
    logs ARRAY(ROW (
        timestamp TIMESTAMP,
        fields ARRAY(ROW(
        key VARCHAR,
        v_type INTEGER,
        v_str VARCHAR,
        v_bool BOOLEAN,
        v_int64 BIGINT,
        v_float64 DOUBLE ))
        )) WITH (layout = 'timestamp=2006-01-02T15:04:05.999999999Z07:00'),
    process ROW(
            service_name VARCHAR,
            tags ARRAY(
                      ROW(
                          key VARCHAR,
                          v_type INTEGER,
                          v_str VARCHAR,
                          v_bool BOOLEAN,
                          v_int64 BIGINT,
                          v_float64 DOUBLE
                          )
                    )
           ),
    process_id     VARCHAR,
    warnings ARRAY (VARCHAR)
)
