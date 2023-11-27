{{ retrieve_canvas_records_from_data_lake('base_canvas_quiz_submissions') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.submission_id')            as submission_id,
    json_value(json_data, '$.user_id')                  as user_id,
    json_value(json_data, '$.quiz_id')                  as quiz_id,
    safe_cast(
        json_value(json_data, '$.quiz_version')
        as int64
    )                                                   as quiz_version,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.started_at')
    )                                                   as started_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.finished_at')
    )                                                   as finished_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.end_at')
    )                                                   as end_at,
    safe_cast(
        json_value(json_data, '$.score')
        as float64
    )                                                   as score,
    safe_cast(
        json_value(json_data, '$.attempt')
        as int64
    )                                                   as attempt,
    safe_cast(
        json_value(json_data, '$.kept_score')
        as float64
    )                                                   as kept_score,
    safe_cast(
        json_value(json_data, '$.fudge_points')
        as float64
    )                                                   as fudge_points,
    safe_cast(
        json_value(json_data, '$.quiz_points_possible')
        as float64
    )                                                   as quiz_points_possible,
    safe_cast(
        json_value(json_data, '$.extra_attempts')
        as int64
    )                                                   as extra_attempts,
    json_value(json_data, '$.temporary_user_code')      as temporary_user_code,
    safe_cast(
        json_value(json_data, '$.extra_time')
        as int64
    )                                                   as extra_time,
    safe_cast(
        json_value(json_data, '$.manually_scored')
        as boolean
    )                                                   as manually_scored,
    safe_cast(
        json_value(json_data, '$.manually_unlocked')
        as boolean
    )                                                   as manually_unlocked,
    safe_cast(
        json_value(json_data, '$.was_preview')
        as boolean
    )                                                   as was_preview,
    safe_cast(
        json_value(json_data, '$.score_before_regrade')
        as float64
    )                                                   as score_before_regrade,
    safe_cast(
        json_value(json_data, '$.has_seen_results')
        as boolean
    )                                                   as has_seen_results,
    json_value(json_data, '$.workflow_state')           as workflow_state,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.created_at')
    )                                                   as created_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.updated_at')
    )                                                   as updated_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.deleted_at')
    )                                                   as deleted_at,
from
    records
