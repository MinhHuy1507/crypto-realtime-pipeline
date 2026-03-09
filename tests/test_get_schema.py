"""Unit tests for get_latest_schema — validates Schema Registry REST API integration."""
import json
import pytest
from unittest.mock import patch, MagicMock
from spark_jobs.dependencies.get_schema import get_latest_schema
import requests


MOCK_SCHEMA = json.dumps({
    "type": "record",
    "name": "TradeEvent",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "price", "type": "double"},
    ]
})


class TestGetLatestSchema:
    @patch("spark_jobs.dependencies.get_schema.requests.get")
    def test_successful_schema_fetch(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"schema": MOCK_SCHEMA}
        )
        mock_get.return_value.raise_for_status = MagicMock()

        result = get_latest_schema(registry_url="http://fake:8081", subject="test-value")

        assert result == MOCK_SCHEMA
        mock_get.assert_called_once_with("http://fake:8081/subjects/test-value/versions/latest")

    @patch("spark_jobs.dependencies.get_schema.requests.get")
    def test_connection_error_raises(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("refused")

        with pytest.raises(requests.exceptions.ConnectionError):
            get_latest_schema(registry_url="http://fake:8081", subject="test-value")

    @patch("spark_jobs.dependencies.get_schema.requests.get")
    def test_404_subject_not_found_raises(self, mock_get):
        mock_response = MagicMock(status_code=404)
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
        mock_get.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError):
            get_latest_schema(registry_url="http://fake:8081", subject="missing-value")
