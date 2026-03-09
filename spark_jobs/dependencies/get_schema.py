import logging
import requests
from config.kafka import SCHEMA_REGISTRY_URL, KAFKA_TOPIC

logger = logging.getLogger(__name__)

SUBJECT = f"{KAFKA_TOPIC}-value"

def get_latest_schema(registry_url : str = SCHEMA_REGISTRY_URL, subject : str = SUBJECT):
    """Fetches the latest Avro schema from Confluent Schema Registry REST API."""
    url = f"{registry_url}/subjects/{subject}/versions/latest"
    try:
        response = requests.get(url)
        response.raise_for_status()
        schema_info = response.json()
        return schema_info['schema']
    
    except requests.exceptions.ConnectionError:
        logger.error(f"Cannot connect to Schema Registry at {registry_url}. Please ensure the schema-registry container is running and on the same network.")
        raise
    
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            logger.error(f"Connection successful but subject '{subject}' not found. Please run the Producer first to register the schema.")
        else:
            logger.error(f"HTTP Error: {e}")
        raise
    
    except Exception as e:
        logger.error(f"Unknown error while retrieving schema: {e}")
        raise e