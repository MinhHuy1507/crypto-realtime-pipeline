from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseWriter(ABC):
    """
    Abstract base class for all Spark writers.
    """
    @abstractmethod
    def write(self, df: DataFrame, **kwargs):
        """
        Writes a DataFrame to a sink.
        """
        pass

class BaseReader(ABC):
    """
    Abstract base class for all Spark readers.
    """
    @abstractmethod
    def read(self, **kwargs) -> DataFrame:
        """
        Reads data from a source into a DataFrame.
        """
        pass
