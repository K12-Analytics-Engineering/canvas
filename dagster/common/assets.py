from dagster_dbt import DbtCliResource, dbt_assets

from dagster import (
    AssetExecutionContext,
    AssetKey,
    FreshnessPolicy,
    MetadataValue,
    Output,
    asset,
)

from .constants import dbt_manifest_path
from .resources import CanvasData2ApiResource


@dbt_assets(manifest=dbt_manifest_path)
def canvas_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


def create_canvasdata2_asset(instance_name, asset_name, namespace, table_name):
    """
    Take in customer instance name
    Return list json objects with response's data property
    """

    @asset(
        name=asset_name,
        group_name="canvasdata2",
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=1440),
        io_manager_key="gcs_io_manager",
        metadata={"path": f"canvasdata2_api/{asset_name}"},
        key_prefix=[instance_name, "staging"],
    )
    def canvasdata2_asset(
        context: AssetExecutionContext, canvasdata2_api_client: CanvasData2ApiResource
    ):
        # get previous materialization events
        try:
            since = None
            last_materialization = context.instance.get_latest_materialization_event(
                asset_key=AssetKey((instance_name, "staging", asset_name))
            )
            # i.e., will make next request as an 'incremental' change request
            last_materialization_metadata = (
                last_materialization.dagster_event.event_specific_data.materialization.metadata
            )
            try:
                query_ts = last_materialization_metadata["query_ts"]
                since = query_ts.value
                context.log.info(f"Previous query at: {since}")
            except KeyError:
                since = None
                context.log.info("Did not find previous query timestamp")
            except AttributeError:
                since = None
                context.log.info(
                    f"previous query timestamp metadata malformed: {query_ts}"
                )
        except Exception as err:
            context.log.info(f"Did not find previous asset materialization: {err}")
            # i.e., never materialized, so will make next request as a full 'snapshot'
            since = None
        result, schema_version, query_ts = canvasdata2_api_client.get_data(
            namespace, table_name, since=since
        )
        return Output(
            value=result,
            metadata={
                "Number of records": MetadataValue.int(len(result)),
                "schema_version": schema_version,
                "query_ts": query_ts,
            },
        )

    return canvasdata2_asset
