{{ retrieve_canvas_records_from_data_lake('base_canvas_roles') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.name')                     as name,        -- noqa: RF04
    json_value(json_data, '$.account_id')               as account_id,
    json_value(json_data, '$.base_role_type')           as base_role_type,
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
