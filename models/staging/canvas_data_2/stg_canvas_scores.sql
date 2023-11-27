{{ retrieve_canvas_records_from_data_lake('base_canvas_scores') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.enrollment_id')                as enrollment_id,
    json_value(json_data, '$.assignment_group_id')          as assignment_group_id,
    json_value(json_data, '$.grading_period_id')            as grading_period_id,
    safe_cast(
        json_value(json_data, '$.course_score')
        as boolean
    )                                                       as course_score,
    safe_cast(
        json_value(json_data, '$.current_points')
        as float64
    )                                                       as current_points,
    safe_cast(
        json_value(json_data, '$.unposted_current_points')
        as float64
    )                                                       as unposted_current_points,
    safe_cast(
        json_value(json_data, '$.final_points')
        as float64
    )                                                       as final_points,
    safe_cast(
        json_value(json_data, '$.unposted_final_points')
        as float64
    )                                                       as unposted_final_points,
    safe_cast(
        json_value(json_data, '$.current_score')
        as float64
    )                                                       as current_score,
    safe_cast(
        json_value(json_data, '$.unposted_current_score')
        as float64
    )                                                       as unposted_current_score,
    safe_cast(
        json_value(json_data, '$.final_score')
        as float64
    )                                                       as final_score,
    safe_cast(
        json_value(json_data, '$.unposted_final_score')
        as float64
    )                                                       as unposted_final_score,
    safe_cast(
        json_value(json_data, '$.override_score')
        as float64
    )                                                       as override_score,
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
