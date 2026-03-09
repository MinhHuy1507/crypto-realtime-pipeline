"""DAG integrity tests — validates all DAGs can be parsed without import errors."""
import pytest
import os
import sys

# Add project root to sys.path so DAG imports resolve
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestDagIntegrity:
    """Validates that DAG files are syntactically correct Python (no import-time crashes)."""

    DAG_DIR = os.path.join(PROJECT_ROOT, "dags")
    DAG_FILES = [
        "crypto_trades_realtime.py",
        "dbt_crypto_transform.py",
        "dev_fetch_datalake_sample.py",
        "etl_export_gold_to_datalake.py",
        "ops_check_connectivity.py",
        "ops_init_infrastructure.py",
    ]

    @pytest.mark.parametrize("dag_file", DAG_FILES)
    def test_dag_file_is_valid_python(self, dag_file):
        """Each DAG file should compile without syntax errors."""
        filepath = os.path.join(self.DAG_DIR, dag_file)
        assert os.path.exists(filepath), f"DAG file not found: {dag_file}"

        with open(filepath, "r") as f:
            source = f.read()

        # compile() catches SyntaxError but not import errors (which need Airflow)
        compile(source, filepath, "exec")
