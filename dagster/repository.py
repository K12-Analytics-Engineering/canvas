import os

from common.assets import canvas_dbt_assets, create_canvasdata2_asset
from common.constants import dbt_project_dir
from common.io_managers import gcs_io_manager
from common.resources import CanvasData2ApiResource
from dagster_dbt import DbtCliResource

from dagster import (
    AssetSelection,
    Definitions,
    EnvVar,
    RunRequest,
    ScheduleDefinition,
    define_asset_job,
    multiprocess_executor,
    resource,
    schedule,
)

INSTANCE_NAME = EnvVar("INSTANCE_NAME")


@resource
def globals():
    return {
        "instance_name": INSTANCE_NAME,
    }


RESOURCES_LOCAL = {
    "gcs_io_manager": gcs_io_manager.configured(
        {
            "gcs_bucket_name": os.environ["GCS_BUCKET_NAME"],
        }
    ),
    "canvasdata2_api_client": CanvasData2ApiResource(
        client_id=EnvVar("CANVASDATA2_CLIENT_ID"),
        client_secret=EnvVar("CANVASDATA2_CLIENT_SECRET"),
    ),
    "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    "globals": globals,
}

RESOURCES_PROD = {
    "gcs_io_manager": gcs_io_manager.configured(
        {
            "gcs_bucket_name": os.environ["GCS_BUCKET_NAME"],
        }
    ),
    "canvasdata2_api_client": CanvasData2ApiResource(
        client_id=EnvVar("CANVASDATA2_CLIENT_ID"),
        client_secret=EnvVar("CANVASDATA2_CLIENT_SECRET"),
    ),
    "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    "globals": globals,
}

resource_defs_by_deployment_name = {
    "prod": RESOURCES_PROD,
    "local": RESOURCES_LOCAL,
}


all_canvasdata2_job = define_asset_job(
    "all_canvasdata2_job",
    selection=AssetSelection.groups("canvasdata2"),
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1", "memory": "4Gi"},
                },
            },
        },
        "dagster/max_retries": 1,
        "dagster/retry_strategy": "ALL_STEPS",
        "instance_name": INSTANCE_NAME,
    },
)


@schedule(
    cron_schedule="0 1 * * *",
    execution_timezone="America/Chicago",
    job=all_canvasdata2_job,
)
def all_canvasdata2_schedule():
    yield RunRequest(run_key=None)


defs = Definitions(
    assets=(
        [
            canvas_dbt_assets,
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_accounts",
                namespace="canvas",
                table_name="accounts",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_course_account_associations",
                namespace="canvas",
                table_name="course_account_associations",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_assignments",
                namespace="canvas",
                table_name="assignments",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_assignment_groups",
                namespace="canvas",
                table_name="assignment_groups",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_assignment_override_students",
                namespace="canvas",
                table_name="assignment_override_students",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_course_sections",
                namespace="canvas",
                table_name="course_sections",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_courses",
                namespace="canvas",
                table_name="courses",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_enrollment_states",
                namespace="canvas",
                table_name="enrollment_states",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_enrollment_terms",
                namespace="canvas",
                table_name="enrollment_terms",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_enrollments",
                namespace="canvas",
                table_name="enrollments",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_grading_periods",
                namespace="canvas",
                table_name="grading_periods",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_pseudonyms",
                namespace="canvas",
                table_name="pseudonyms",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_quiz_submissions",
                namespace="canvas",
                table_name="quiz_submissions",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_roles",
                namespace="canvas",
                table_name="roles",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_scores",
                namespace="canvas",
                table_name="scores",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_submissions",
                namespace="canvas",
                table_name="submissions",
            ),
            create_canvasdata2_asset(
                instance_name=INSTANCE_NAME,
                asset_name="base_canvas_users",
                namespace="canvas",
                table_name="users",
            ),
        ]
    ),
    schedules=[
        all_canvasdata2_schedule,
        ScheduleDefinition(
            job=define_asset_job(
                "assets_job",
                selection=AssetSelection.all() - AssetSelection.groups("canvasdata2"),
                tags={
                    "dagster-k8s/config": {
                        "container_config": {
                            "resources": {
                                "requests": {"cpu": "750m", "memory": "2Gi"},
                            }
                        },
                    },
                    "dagster/max_retries": 1,
                    "dagster/retry_strategy": "ALL_STEPS",
                    "instance_name": INSTANCE_NAME,
                },
            ),
            cron_schedule="0 2 * * *",
            execution_timezone="America/Chicago",
        ),
    ],
    jobs=[all_canvasdata2_job],
    sensors=[],
    resources=resource_defs_by_deployment_name[
        os.environ.get("DAGSTER_DEPLOYMENT", "local")
    ],
    executor=multiprocess_executor.configured({"max_concurrent": 2}),
)
