from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config.kafka import KAFKA_BOOTSTRAP_SERVERS
import logging

logger = logging.getLogger(__name__)

def check_kafka_connection():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='airflow_admin'
        )
        admin_client.list_topics()
        logger.info(f"Kafka Connection Successful to {KAFKA_BOOTSTRAP_SERVERS}")
    except Exception as e:
        logger.error(f"Kafka Connection Failed: {e}")

def create_kafka_topic(topic_name: str, num_partitions: int = 2, replication_factor: int = 1):
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='airflow_admin'
    )
    topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        logger.warning(f"Topic '{topic_name}' already exists. Skipping.")
    except Exception as e:
        logger.error(f"Failed to create topic: {e}")
        raise e
    finally:
        admin_client.close()

def check_kafka_topic_exists(topic_name: str) -> bool:
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='airflow_admin'
    )

    try:
        topic_metadata = admin_client.list_topics()
        exists = topic_name in topic_metadata
        logger.info(f"Topic '{topic_name}' exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Failed to check topic existence: {e}")
        raise e
    finally:
        admin_client.close()