{{ retrieve_canvas_records_from_data_lake('base_canvas_enrollment_states', 'enrollment_id') }}

select
    enrollment_id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.state')                    as state,
    safe_cast(
        json_value(json_data, '$.state_is_current')
        as boolean
    )                                                   as state_is_current,
    safe_cast(
        json_value(json_data, '$.restricted_access')
        as boolean
    )                                                   as restricted_access,
    safe_cast(
        json_value(json_data, '$.access_is_current')
        as boolean
    )                                                   as access_is_current,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.state_started_at')
    )                                                   as state_started_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.state_valid_until')
    )                                                   as state_valid_until,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.updated_at')
    )                                                   as updated_at,
from
    records
