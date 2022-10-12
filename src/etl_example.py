import json
from datetime import datetime

import pandas as pd
import requests
from prefect import flow, task


@task
def extract(url: str) -> dict:
    """extract data from url

    Args:
        url (str): url in string

    Raises:
        Exception: unless nothing is fetched

    Returns:
        dict: data stored in dictionary format
    """
    res = requests.get(url)
    if not res:
        raise Exception("No data fetched!")
    return json.loads(res.content)


@task
def transform(data: dict) -> pd.DataFrame:
    """Transform dictionary to dataframe

    Args:
        data (dict): fetched from url

    Returns:
        pd.DataFrame: transformed dataframe
    """
    transformed = []
    for user in data:
        transformed.append(
            {
                "ID": user["id"],
                "Name": user["name"],
                "Username": user["username"],
                "Email": user["email"],
                "Address": f"{user['address']['street']}, \
                    {user['address']['suite']}, \
                        {user['address']['city']}",
                "PhoneNumber": user["phone"],
                "Company": user["company"]["name"],
            }
        )
    return pd.DataFrame(transformed)


@task
def load(data: pd.DataFrame, path: str) -> None:
    """Store transformed data in csv format

    Args:
        data (pd.DataFrame): from previous step
        path (str): designated folder ../data
    """
    data.to_csv(path_or_buf=path, index=False)


@flow(name="simple_etl_pipeline")
def prefect_flow():
    """orchestrate the entire flow"""
    users = extract(url="https://jsonplaceholder.typicode.com/users")
    df_users = transform(users)
    load(
        data=df_users,
        path="../data/users_" + str(int(datetime.now().timestamp())) + ".csv",
    )


if __name__ == "__main__":
    prefect_flow()
