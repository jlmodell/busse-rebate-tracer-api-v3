import datetime
import pandas as pd
from functools import lru_cache
import re
from rich import print
import httpx

from constants import CONTRACTS, PRICING_CONTRACTS, TRACINGS, DISCORD_URL
from database import gc_rbt, gc_bp, find_contract_by_contract_number
from s3_functions import (
    save_df_to_s3_as_excel,
    save_tracings_df_as_html_with_javascript_css,
)
from .tracings import *


@lru_cache(maxsize=None)
def find_contract_price(contract: str = None, part: str = None) -> float:
    assert contract is not None, "contract cannot be None"

    contract = contract.upper().strip()
    part = part.upper().strip()

    collection = gc_bp(PRICING_CONTRACTS)

    res = collection.find_one(
        {
            "contractnumber": contract,
            "pricingagreements.item": part,
        },
        {
            "_id": 0,
            "pricingagreements": {
                "$elemMatch": {
                    "item": part,
                }
            },
        },
    )

    return res["pricingagreements"][0]["price"] if res else 0.0


@lru_cache(maxsize=None)
def find_contract_name(contract: str = None) -> str:
    assert contract is not None, "contract cannot be None"

    contract = contract.upper().strip()

    collection = gc_bp(PRICING_CONTRACTS)

    res = collection.find_one(
        {
            "contractnumber": contract,
        },
        {
            "_id": 0,
            "contractname": 1,
        },
    )

    return res["contractname"] if res else contract


@lru_cache(maxsize=None)
def find_contract_end_date(contract: str = None) -> str:
    assert contract is not None, "contract cannot be None"

    contract = contract.upper().strip()

    collection = gc_bp(PRICING_CONTRACTS)

    res = collection.find_one(
        {
            "contractnumber": contract,
        },
        {
            "_id": 0,
            "contractend": 1,
        },
    )

    contractend = "UNKNOWN"

    if res:
        contractend = res["contractend"]
        if type(contractend) is datetime.datetime:
            contractend = "{:%Y-%m-%d}".format(contractend)

    return contractend[0:10]


def find_tracings_by_period(period: str = None) -> pd.DataFrame:
    assert period is not None, "period cannot be None"

    aggregation = []

    aggregation.append({"$match": {"period": {"$regex": period, "$options": "i"}}})

    aggregation.append(
        {
            "$lookup": {
                "from": "sched_data",
                "localField": "part",
                "foreignField": "part",
                "as": "part_data",
            }
        }
    )

    collection = gc_rbt(TRACINGS)

    res = list(collection.aggregate(aggregation))

    df = pd.DataFrame(res)

    contracts_unique = df["contract"].unique()

    contracts = {}
    for contract in contracts_unique:
        contracts[contract] = find_contract_by_contract_number(contract)
        if contracts[contract] is not None:
            contracts[contract] = {
                x["item"]: x["price"] for x in contracts[contract]["pricingagreements"]
            }
        else:
            contracts[contract] = {}
            print(contract, "not found")

    df["contract_price"] = df.apply(
        lambda x: contracts.get(x["contract"]).get(x["part"], 0.0), axis=1
    )

    df["contract_name"] = df["contract"].apply(find_contract_name)
    df["contract_end"] = df["contract"].apply(find_contract_end_date)

    return df


def find_tracings_and_save(
    month: str = None, year: str = None, overwrite: bool = False
) -> pd.DataFrame:
    months = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ]
    re_year = re.compile(r"^\d{4}$")

    assert month is not None, "month cannot be None"
    assert month.lower() in months, "month must be one of: {}".format(months)
    assert year is not None, "year cannot be None"
    assert re_year.match(year), "year must be a 4-digit number"

    period = month.upper().strip() + year.upper().strip()

    print("Period \t> ", period)

    df = find_tracings_by_period(period)
    df = CLEANUP_TRACINGS_DF(df)

    df, df_with_summary = TRANSFORM_BY_DISTRIBUTOR_BY_CONTRACT_BY_PART(df)

    if overwrite:
        save_tracings_df_as_html_with_javascript_css(
            df_with_summary, f"output/{period}/", "dist_by_contract_by_part.html"
        )
        save_df_to_s3_as_excel(
            df, f"output/{period}/", "tracings_by_distributor_by_contract_by_part.xlsm"
        )

    content = f"Tracings for {period} are ready."

    httpx.post(DISCORD_URL, json={"content": content})

    return df_with_summary
