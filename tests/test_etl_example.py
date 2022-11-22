import json
import os.path
import sys

import pytest
import requests

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import List

from prefect.testing.utilities import prefect_test_harness

from src.etl_example import prefect_flow, transform

testurl = ["https://jsonplaceholder.typicode.com/comments"]


@pytest.mark.parametrize("testurl", testurl)
def test_extract(testurl: str) -> None:
    """_summary_

    Args:
        testurl (str): _description_
    """
    res = requests.get(testurl)
    assert len(json.loads(res.content)) != 0
    assert isinstance(json.loads(res.content), Iterable)
    # assert isinstance(json.loads(res.content)[0],dict)
    for i in json.loads(res.content):
        assert isinstance(i, dict)


@pytest.fixture
def example_data() -> List:
    """_summary_

    Returns:
        List: _description_
    """
    return [
        {
            "id": 1,
            "name": "Leanne Graham",
            "username": "Bret",
            "email": "Sincere@april.biz",
            "address": {
                "street": "Kulas Light",
                "suite": "Apt. 556",
                "city": "Gwenborough",
                "zipcode": "92998-3874",
                "geo": {"lat": "-37.3159", "lng": "81.1496"},
            },
            "phone": "1-770-736-8031 x56442",
            "website": "hildegard.org",
            "company": {
                "name": "Romaguera-Crona",
                "catchPhrase": "Multi-layered client-server neural-net",
                "bs": "harness real-time e-markets",
            },
        }
    ]


def test_transform(example_data: List) -> None:
    """_summary_

    Args:
        example_data (List): _description_
    """
    assert len(transform.fn(example_data)) != 0


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture() -> None:
    """_summary_"""
    with prefect_test_harness():
        yield


def test_prefect_flow() -> None:
    """_summary_"""
    prefect_flow()
    path = Path(
        "/workspaces/ds-example-workflow/data/users_"
        + str(int(datetime.now().timestamp()))
        + ".csv"
    )
    assert path.is_file()
