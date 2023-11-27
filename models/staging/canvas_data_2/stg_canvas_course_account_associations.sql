{{ retrieve_canvas_records_from_data_lake('base_canvas_course_account_associations') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.course_id')            as course_id,
    json_value(json_data, '$.course_section_id')    as course_section_id,
    json_value(json_data, '$.account_id')           as account_id,
    safe_cast(
        json_value(json_data, '$.depth')
        as int64
    )                                               as depth,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.created_at')
    )                                               as created_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.updated_at')
    )                                               as updated_at,
from
    records
