{{ retrieve_canvas_records_from_data_lake('base_canvas_accounts') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.uuid')                     as uuid,
    json_value(json_data, '$.name')                     as name,                -- noqa: RF04
    json_value(json_data, '$.parent_account_id')        as parent_account_id,
    json_value(json_data, '$.sis_source_id')            as sis_source_id,
    json_value(json_data, '$.current_sis_batch_id')     as current_sis_batch_id,
    json_value(json_data, '$.default_locale')           as default_locale,
    json_value(json_data, '$.default_time_zone')        as default_time_zone,
    json_value(json_data, '$.course_template_id')       as course_template_id,
    json_value(json_data, '$.lti_context_id')           as lti_context_id,
    json_value(json_data, '$.integration_id')           as integration_id,
    json_value(json_data, '$.consortium_parent_account_id') as consortium_parent_account_id,
    safe_cast(
        json_value(json_data, '$.storage_quota')
        as int64
    )                                                   as storage_quota,
    safe_cast(
        json_value(json_data, '$.default_storage_quota')
        as int64
    )                                                   as default_storage_quota,
    safe_cast(
        json_value(json_data, '$.default_user_storage_quota')
        as int64
    )                                                   as default_user_storage_quota,
    safe_cast(
        json_value(json_data, '$.default_group_storage_quota')
        as int64
    )                                                   as default_group_storage_quota,
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
