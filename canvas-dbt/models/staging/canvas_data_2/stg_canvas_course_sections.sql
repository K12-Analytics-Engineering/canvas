{{ retrieve_canvas_records_from_data_lake('base_canvas_course_sections') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.name')             as course_section_name,
    json_value(json_data, '$.course_id')        as course_id,
    json_value(json_data, '$.sis_batch_id')     as sis_batch_id,
    json_value(json_data, '$.sis_source_id')    as sis_source_id,
    json_value(json_data, '$.integration_id')   as integration_id,
    safe_cast(
        json_value(json_data, '$.restrict_enrollments_to_section_dates')
        as boolean
    )                                           as restrict_enrollments_to_section_dates,
    safe_cast(
        json_value(json_data, '$.default_section')
        as boolean
    )                                           as default_section,
    safe_cast(
        json_value(json_data, '$.accepting_enrollments')
        as boolean
    )                                           as accepting_enrollments,
    safe_cast(
        json_value(json_data, '$.nonxlist_course_id')
        as int64
    )                                           as nonxlist_course_id,
    safe_cast(
        json_value(json_data, '$.enrollment_term_id')
        as int64
    )                                           as enrollment_term_id,
    json_value(json_data, '$.workflow_state')   as workflow_state,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.start_at')
    )                                           as start_date,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.end_at')
    )                                           as end_end,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.created_at')
    )                                           as created_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.updated_at')
    )                                           as updated_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.deleted_at')
    )                                           as deleted_at,
from
    records
