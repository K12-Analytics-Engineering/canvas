{{ retrieve_canvas_records_from_data_lake('base_canvas_users') }}

select
    id,
    date_metadata,
    date_extracted,
    safe_cast(
        json_value(json_data, '$.storage_quota')
        as int64
    )                                                   as storage_quota,
    json_value(json_data, '$.lti_context_id')           as lti_context_id,
    json_value(json_data, '$.sortable_name')            as sortable_name,
    json_value(json_data, '$.avatar_image_url')         as avatar_image_url,
    json_value(json_data, '$.avatar_image_source')      as avatar_image_source,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.avatar_image_updated_at')
    )                                                   as avatar_image_updated_at,
    json_value(json_data, '$.short_name')               as short_name,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.last_logged_out')
    )                                                   as last_logged_out,
    json_value_array(json_data, '$.pronouns')           as pronouns,
    json_value(json_data, '$.merged_into_user_id')      as merged_into_user_id,
    json_value(json_data, '$.locale')                   as locale,
    json_value(json_data, '$.name')                     as name,        -- noqa: RF04
    json_value(json_data, '$.time_zone')                as time_zone,
    json_value(json_data, '$.uuid')                     as uuid,
    json_value(json_data, '$.school_name')              as school_name,
    json_value(json_data, '$.school_position')          as school_position,
    safe_cast(
        json_value(json_data, '$.storage_quota')
        as boolean
    )                                                   as public,      -- noqa: RF04
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
