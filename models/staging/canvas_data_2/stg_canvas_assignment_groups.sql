{{ retrieve_canvas_records_from_data_lake('base_canvas_assignment_groups') }}

select
    id,
    date_metadata,
    date_extracted,
    json_value(json_data, '$.name')                     as name,            -- noqa: RF04
    json_value(json_data, '$.context_id')               as context_id,
    json_value(json_data, '$.context_type')             as context_type,
    json_value(json_data, '$.default_assignment_name')  as default_assignment_name,
    safe_cast(
        json_value(json_data, '$.group_weight')
        as float64
    )                                                   as group_weight,
    json_value(json_data, '$.migration_id')             as migration_id,
    json_value(json_data, '$.sis_source_id')            as sis_source_id,
    json_value(json_data, '$.position')                 as position,
    safe_cast(
        json_value(json_data, '$.rules.drop_highest')
        as int64
    )                                                   as drop_highest,
    safe_cast(
        json_value(json_data, '$.rules.drop_lowest')
        as int64
    )                                                   as drop_lowest,
    json_value_array(json_data, '$.rules.never_drop')   as never_drop,
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
