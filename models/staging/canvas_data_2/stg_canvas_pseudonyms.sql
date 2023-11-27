{{ retrieve_canvas_records_from_data_lake('base_canvas_pseudonyms') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.integration_id')           as integration_id,
    json_value(json_data, '$.user_id')                  as user_id,
    json_value(json_data, '$.account_id')               as account_id,
    json_value(json_data, '$.sis_batch_id')             as sis_batch_id,
    json_value(json_data, '$.unique_id')                as unique_id,
    safe_cast(
        json_value(json_data, '$.login_count')
        as int64
    )                                                   as login_count,
    safe_cast(
        json_value(json_data, '$.failed_login_count')
        as int64
    )                                                   as failed_login_count,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.last_request_at')
    )                                                   as last_request_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.last_login_at')
    )                                                   as last_login_at,
    parse_timestamp(
        '%Y-%m-%dT%H:%M:%E3SZ',
        json_value(json_data, '$.current_login_at')
    )                                                   as current_login_at,
    json_value(json_data, '$.last_login_ip')            as last_login_ip,
    json_value(json_data, '$.current_login_ip')         as current_login_ip,
    json_value(json_data, '$.sis_user_id')              as sis_user_id,
    json_value(json_data, '$.authentication_provider_id') as authentication_provider_id,
    safe_cast(
        json_value(json_data, '$.position')
        as int64
    )                                                   as position,
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
