{{ retrieve_canvas_records_from_data_lake('base_canvas_courses') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.uuid')                     as course_uuid,
    json_value(json_data, '$.course_code')              as course_code,
    json_value(json_data, '$.name')                     as course_name,
    json_value(json_data, '$.account_id')               as account_id,
    json_value(json_data, '$.enrollment_term_id')       as enrollment_term_id,
    json_value(json_data, '$.grading_standard_id')      as grading_standard_id,
    json_value(json_data, '$.sis_source_id')            as sis_source_id,
    json_value(json_data, '$.sis_batch_id')             as sis_batch_id,
    json_value(json_data, '$.group_weighting_scheme')   as group_weighting_scheme,
    safe_cast(
        json_value(json_data, '$.is_public')
        as boolean
    )                                                   as is_public,
    json_value(json_data, '$.wiki_id')                  as wiki_id,
    safe_cast(
        json_value(json_data, '$.homeroom_course')
        as boolean
    )                                                   as homeroom_course,
    json_value(json_data, '$.lti_context_id')           as lti_context_id,
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
