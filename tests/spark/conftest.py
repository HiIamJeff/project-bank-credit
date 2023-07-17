
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request):
    """ Create a spark session for testing
    """
    spark = (SparkSession.builder
             .master("local[1]") # setting for local mode
             .appName('test-monthly-process')
             .getOrCreate())
    yield spark
