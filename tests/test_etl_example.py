import sys
import os.path
import pandas as pd
import json
import requests
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from src.etl_example import (
    extract,
    transform,
    load,
    prefect_flow
)
from typing import Dict, List
from collections.abc import Iterable
import pytest


testurl = ['https://jsonplaceholder.typicode.com/comments']

@pytest.mark.parametrize('testurl', testurl)

def test_extract(testurl: str):
    res = requests.get(testurl)
    assert len(json.loads(res.content))!=0
    assert isinstance(json.loads(res.content), Iterable)
    # assert isinstance(json.loads(res.content)[0],dict)
    for i in json.loads(res.content):
        assert isinstance(i,dict)

# @pytest.fixture
# def example_data():
#     return [
#             {
#                 "id": 1,
#                 "name": "Leanne Graham",
#                 "username": "Bret",
#                 "email": "Sincere@april.biz",
#                 "address": {
#                 "street": "Kulas Light",
#                 "suite": "Apt. 556",
#                 "city": "Gwenborough",
#                 "zipcode": "92998-3874",
#                 "geo": {
#                     "lat": "-37.3159",
#                     "lng": "81.1496"
#                 }
#                 },
#                 "phone": "1-770-736-8031 x56442",
#                 "website": "hildegard.org",
#                 "company": {
#                 "name": "Romaguera-Crona",
#                 "catchPhrase": "Multi-layered client-server neural-net",
#                 "bs": "harness real-time e-markets"
#                 }
#             }
#             ]