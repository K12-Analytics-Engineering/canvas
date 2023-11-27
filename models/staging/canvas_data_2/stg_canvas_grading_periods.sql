{{ retrieve_canvas_records_from_data_lake('base_canvas_grading_periods') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.title')            as title,
    safe_cast(
        json_value(json_data, '$.weight')
        as float64
    )                                           as weight,
    json_value(
        json_data, '$.grading_period_group_id'
    )                                           as grading_period_group_id,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.start_date')
    )                                           as start_date,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.end_date')
    )                                           as end_date,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.close_date')
    )                                           as close_date,
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
