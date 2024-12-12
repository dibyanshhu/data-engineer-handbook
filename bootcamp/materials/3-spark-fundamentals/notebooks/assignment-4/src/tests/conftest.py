'''this module will be used for setting up the spark session'''

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    '''boilerplated code for spark session'''
    return SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()
