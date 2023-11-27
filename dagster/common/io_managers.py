import datetime
import json

from google.cloud import storage

from dagster import (
    InputContext,
    IOManager,
    MetadataValue,
    OutputContext,
    get_dagster_logger,
    io_manager,
)

storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB


class GcsIOManager(IOManager):
    def __init__(self, gcs_bucket_name: str):
        self.gcs_bucket_name = gcs_bucket_name
        self.log = get_dagster_logger()

    def _get_path(self, context) -> str:
        if context.has_partition_key:
            return "/".join(context.asset_key.path + [context.asset_partition_key])
        else:
            return "/".join(context.asset_key.path)

    def handle_output(self, context: OutputContext, obj):
        path = context.metadata["path"]
        self.log.debug(f"Retrieved path: {path}")

        if context.has_partition_key:
            path = (
                path
                + "/partition_key="
                + "/partition_key_two=".join(context.asset_partition_key.split("|"))
            )

        utc_timestamp = datetime.datetime.now()

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.gcs_bucket_name, timeout=300.0)

        file_number = 1
        if len(obj) == 0:
            bucket.blob(
                f"{path}/date_extracted={utc_timestamp}/{abs(hash(utc_timestamp))}-{file_number}.json"
            ).upload_from_string(
                "{}",
                content_type="application/json",
                num_retries=3,
            )
        else:
            for i in range(0, len(obj), 25000):
                bucket.blob(
                    f"{path}/date_extracted={utc_timestamp}/{abs(hash(utc_timestamp))}-{file_number}.json"
                ).upload_from_string(
                    "\r\n".join(
                        [json.dumps(record) for record in obj[i : i + 25000]]
                    ),  # upload a json new line delimited string
                    content_type="application/json",
                    num_retries=3,
                )
                gcs_upload_path = f"gs://{self.gcs_bucket_name}/{path}/date_extracted={utc_timestamp}/{abs(hash(utc_timestamp))}-{file_number}.json"
                self.log.debug(f"Uploaded JSON file to {gcs_upload_path}")
                file_number = file_number + 1

        context.add_output_metadata(
            {
                "Number of records": MetadataValue.int(len(obj)),
                "GCS path": MetadataValue.text(
                    f"gs://{self.gcs_bucket_name}/{path}/date_extracted={utc_timestamp}/{abs(hash(utc_timestamp))}-*.json",
                ),
                "Relative GCS path": MetadataValue.text(
                    f"{path}/date_extracted={utc_timestamp}",
                ),
                "path": MetadataValue.text(
                    context.metadata["path"],
                ),
            }
        )

    def load_input(self, context: InputContext):
        storage_client = storage.Client()
        asset_key = context.asset_key
        self.log.debug(f"Retreived asset key {asset_key}")

        event_log_entry = (
            context.step_context.instance.get_latest_materialization_event(asset_key)
        )
        metadata = (
            event_log_entry.dagster_event.event_specific_data.materialization.metadata
        )
        gcs_path = metadata["Relative GCS path"].value

        if context.has_partition_key:
            path = metadata["path"].value
            self.log.debug(f"Retreived path {path}")
            path = path + f"/partition_key={context.asset_partition_key}"
            self.log.debug(f"path is now {path}")
            blob_names = [
                blob.name
                for blob in storage_client.list_blobs(self.gcs_bucket_name, prefix=path)
            ]
            blob_names.sort(reverse=True)
            gcs_path = blob_names[0]

        blob_string = ""
        for blob in storage_client.list_blobs(self.gcs_bucket_name, prefix=gcs_path):
            self.log.info(f"Retrieving data from {blob}")
            # file_blob = storage.Blob(blob, bucket)
            blob_string = blob_string + blob.download_as_string().decode()

        return [json.loads(row) for row in blob_string.split("\n") if row]


@io_manager(config_schema={"gcs_bucket_name": str})
def gcs_io_manager(context):
    return GcsIOManager(
        context.resource_config["gcs_bucket_name"],
    )
