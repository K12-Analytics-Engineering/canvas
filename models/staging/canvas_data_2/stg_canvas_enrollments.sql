{{ retrieve_canvas_records_from_data_lake('base_canvas_enrollments') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.user_id')                  as user_id,
    json_value(json_data, '$.course_id')                as course_id,
    json_value(json_data, '$.course_section_id')        as course_section_id,
    json_value(json_data, '$.role_id')                  as role_id,
    json_value(json_data, '$.sis_batch_id')             as sis_batch_id,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.start_at')
    )                                                   as start_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.end_at')
    )                                                   as end_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.completed_at')
    )                                                   as completed_at,
    json_value(json_data, '$.sis_pseudonym_id')         as sis_pseudonym_id,
    json_value(json_data, '$.type')                     as type,            -- noqa: RF04
    json_value(json_data, '$.grade_publishing_status')  as grade_publishing_status,
    json_value(json_data, '$.associated_user_id')       as associated_user_id,
    safe_cast(
        json_value(json_data, '$.self_enrolled')
        as boolean
    )                                                   as self_enrolled,
    safe_cast(
        json_value(json_data, '$.limit_privileges_to_course_section')
        as boolean
    )                                                   as limit_privileges_to_course_section,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.last_activity_at')
    )                                                   as last_activity_at,
    safe_cast(
        json_value(json_data, '$.total_activity_time')
        as int64
    )                                                   as total_activity_time,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.last_attended_at')
    )                                                   as last_attended_at,
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
