{{ retrieve_canvas_records_from_data_lake('base_canvas_assignments') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.title')                        as assignment_title,
    safe_cast(
        json_value(json_data, '$.points_possible')
        as float64
    )                                                       as points_possible,
    json_value(json_data, '$.grading_type')                 as grading_type,
    json_value(json_data, '$.assignment_group_id')          as assignment_group_id,
    json_value(json_data, '$.context_id')                   as context_id,
    json_value(json_data, '$.context_type')                 as context_type,
    safe_cast(
        json_value(json_data, '$.omit_from_final_grade')
        as boolean
    )                                                       as omit_from_final_grade,
    safe_cast(
        json_value(json_data, '$.grader_count')
        as int64
    )                                                       as grader_count,
    safe_cast(
        json_value(json_data, '$.position')
        as int64
    )                                                       as position,
    json_value(json_data, '$.workflow_state')               as workflow_state,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.created_at')
    )                                                       as created_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.updated_at')
    )                                                       as updated_at,
from
    records
