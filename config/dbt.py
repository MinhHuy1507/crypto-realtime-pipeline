from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = "/opt/airflow/dbt"

PROJECT_CONFIG = ProjectConfig(DBT_PROJECT_PATH)

PROFILE_CONFIG = ProfileConfig(
    profile_name="crypto_pipeline",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="POSTGRES_DW",
        profile_args={"schema": "raw"},
    ),
)

EXECUTION_CONFIG = ExecutionConfig(dbt_executable_path="dbt")

RENDER_CONFIG = RenderConfig(
    test_behavior=TestBehavior.AFTER_ALL,
    dbt_deps=False
)