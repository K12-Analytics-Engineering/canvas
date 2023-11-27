{{ retrieve_canvas_records_from_data_lake('base_canvas_assignment_override_students') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.user_id')                  as user_id,
    json_value(json_data, '$.assignment_id')            as assignment_id,
    json_value(json_data, '$.quiz_id')                  as quiz_id,
    json_value(json_data, '$.assignment_override_id')   as assignment_override_id,
    json_value(json_data, '$.workflow_state')           as workflow_state,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.created_at')
    )                                                   as created_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.updated_at')
    )                                                   as updated_at,
from
    records
