{{ retrieve_canvas_records_from_data_lake('base_canvas_enrollment_terms') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.name')             as name,        -- noqa: RF04
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.start_at')
    )                                           as start_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.end_at')
    )                                           as end_at,
    json_value(json_data, '$.sis_source_id')    as sis_source_id,
    json_value(json_data, '$.sis_batch_id')     as sis_batch_id,
    json_value(json_data, '$.integration_id')   as integration_id,
    json_value(json_data, '$.term_code')        as term_code,
    json_value(json_data, '$.grading_period_group_id') as grading_period_group_id,
    json_value(json_data, '$.workflow_state')   as workflow_state,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.created_at')
    )                                           as created_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.updated_at')
    )                                           as updated_at,
from
    records
