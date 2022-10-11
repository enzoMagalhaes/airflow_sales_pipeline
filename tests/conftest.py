import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dagbag():
    return DagBag()

@pytest.fixture(scope="session")
def bix_dag():
    return DagBag().get_dag('bix_etl')
