{{ retrieve_canvas_records_from_data_lake('base_canvas_submissions') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.course_id')                as course_id,
    json_value(json_data, '$.user_id')                  as user_id,
    json_value(json_data, '$.assignment_id')            as assignment_id,
    json_value(json_data, '$.attachment_id')            as attachment_id,
    json_value(json_data, '$.attachment_ids')           as attachment_ids,
    json_value(json_data, '$.media_comment_id')         as media_comment_id,
    json_value(json_data, '$.media_comment_type')       as media_comment_type,
    json_value(json_data, '$.group_id')                 as group_id,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.cached_due_date')
    )                                                   as cached_due_date,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.posted_at')
    )                                                   as posted_at,
    json_value(json_data, '$.submission_type')          as submission_type,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.submitted_at')
    )                                                   as submitted_at,
    json_value(json_data, '$.quiz_submission_id')       as quiz_submission_id,
    safe_cast(
        json_value(json_data, '$.extra_attempts')
        as int64
    )                                                   as extra_attempts,
    json_value(json_data, '$.grading_period_id')        as grading_period_id,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.graded_at')
    )                                                   as graded_at,
    json_value(json_data, '$.grader_id')                as grader_id,
    safe_cast(
        json_value(json_data, '$.submission_comments_count')
        as int64
    )                                                   as submission_comments_count,
    json_value(json_data, '$.media_object_id')          as media_object_id,
    json_value(json_data, '$.turnitin_data')            as turnitin_data,
    safe_cast(
        json_value(json_data, '$.graded_anonymously')
        as boolean
    )                                                   as graded_anonymously,
    json_value(json_data, '$.anonymous_id')             as anonymous_id,
    json_value(json_data, '$.late_policy_status')       as late_policy_status,
    safe_cast(
        json_value(json_data, '$.points_deducted')
        as float64
    )                                                   as points_deducted,
    safe_cast(
        json_value(json_data, '$.seconds_late_override')
        as int64
    )                                                   as seconds_late_override,
    safe_cast(
        json_value(json_data, '$.student_entered_score')
        as float64
    )                                                   as student_entered_score,
    safe_cast(
        json_value(json_data, '$.score')
        as float64
    )                                                   as score,
    safe_cast(
        json_value(json_data, '$.grade')
        as float64
    )                                                   as grade,
    safe_cast(
        json_value(json_data, '$.published_score')
        as float64
    )                                                   as published_score,
    safe_cast(
        json_value(json_data, '$.published_grade')
        as float64
    )                                                   as published_grade,
    safe_cast(
        json_value(json_data, '$.grade_matches_current_submission')
        as boolean
    )                                                   as grade_matches_current_submission,
    safe_cast(
        json_value(json_data, '$.excused')
        as boolean
    )                                                   as excused,
    json_value(json_data, '$.attempt')                  as attempt,
    safe_cast(
        json_value(json_data, '$.processed')
        as boolean
    )                                                   as processed,
    safe_cast(
        json_value(json_data, '$.cached_quiz_lti')
        as boolean
    )                                                   as cached_quiz_lti,
    json_value(json_data, '$.cached_tardiness')         as cached_tardiness,
    json_value(json_data, '$.lti_user_id')              as lti_user_id,
    safe_cast(
        json_value(json_data, '$.redo_request')
        as boolean
    )                                                   as redo_request,
    json_value(json_data, '$.resource_link_lookup_uuid') as resource_link_lookup_uuid,
    json_value(json_data, '$.body')                     as body,
    json_value(json_data, '$.url')                      as url,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.last_comment_at')
    )                                                   as last_comment_at,
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
