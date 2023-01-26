import os
import re
from datetime import datetime
from functools import lru_cache
from glob import glob

import pandas as pd
from pymongo.collection import Collection
from rich import print

from constants import DATA_WAREHOUSE, ROSTERS, SCHED_DATA, VHA_VIZIENT_MEDASSETS
from database import (
    delete_documents,
    gc_rbt,
    get_current_contracts,
    get_documents,
    insert_documents,
)
from s3_functions import SET_COLUMNS, get_field_file_body_and_decode_kwargs

from .ingest import *
from .load import *

roster_collection = gc_rbt(ROSTERS)


def ingest_to_data_warehouse(
    year: str,
    month: str,
    delimiter: str = ",",
    header_row: int = 0,
    overwrite: bool = False,
    file_path: str = None,
    prefix: str = None,
    key: str = None,
) -> pd.DataFrame:

    assert year is not None, "year is required"
    assert month is not None, "month is required"

    if prefix is not None and key is not None:
        dtype = GET_DTYPES_S3(
            prefix=prefix, key=key, delimiter=delimiter, header_row=header_row
        )

        assert len(dtype) > 0, "No columns found"

        if re.search(r".xl(sx|sm|s)$", key, re.IGNORECASE):
            df = pd.read_excel(
                io.BytesIO(
                    CLIENT.get_object(Bucket=S3_BUCKET, Key=prefix + key)["Body"].read()
                ),
                header=header_row if header_row != -1 else None,
                dtype=dtype,
            )

        elif re.search(r".(csv|txt)$", key, re.IGNORECASE):
            df = pd.read_csv(
                io.BytesIO(
                    CLIENT.get_object(Bucket=S3_BUCKET, Key=prefix + key)["Body"].read()
                ),
                delimiter=delimiter,
                header=header_row if header_row != -1 else None,
                dtype=dtype,
            )

        df = GET_CLEAN_DF_TO_INGEST(
            df=df, file_path=prefix + key, month=month, year=year
        )

    else:

        assert file_path is not None, "file_path is required"
        assert os.path.exists(file_path) is True, "File not found"

        dtype = GET_DTYPES(file_path, delimiter=delimiter, header_row=header_row)

        assert len(dtype) > 0, "No columns found"

        if re.search(r"xl(s|sx|sm)$", file_path, re.IGNORECASE):
            df = pd.read_excel(
                file_path, dtype=dtype, header=header_row if header_row != -1 else None
            )

            try:
                df["Invoice Date"] = df["Invoice Date"].str.rstrip(".0")
                df["Invoice #"] = df["Invoice #"].str.rstrip(".0")
            except:
                pass

        elif re.search(r"(csv|txt)$", file_path, re.IGNORECASE):
            df = pd.read_csv(
                file_path,
                dtype=dtype,
                delimiter=delimiter,
                header=header_row if header_row != -1 else None,
            )

        df = GET_CLEAN_DF_TO_INGEST(df=df, file_path=file_path, month=month, year=year)

    print(df)

    if overwrite:
        collection = gc_rbt(DATA_WAREHOUSE)

        print("Overwriting")

        try:
            file_path = os.path.basename(file_path)
        except TypeError:
            file_path = key.split("/")[-1]

        delete_documents(
            collection,
            {
                "__file__": file_path,
                "__year__": year,
                "__month__": month,
            },
        )

        insert_documents(collection, df.to_dict("records"))

    return df


def ingest_concordance_data_files(
    folder_path: str, year: str, month: str, overwrite: bool = False
):
    assert folder_path is not None, "folder path is required"

    file_paths = glob(os.path.join(folder_path, "*.xls*"))

    sum_total = 0

    for file_path in file_paths:
        df = ingest_to_data_warehouse(
            file_path=file_path,
            year=year,
            month=month,
            overwrite=overwrite,
        )

        sum_total += df["REBATE $"].sum()

    print(sum_total)


@lru_cache(maxsize=None)
def find_license(
    collection: Collection = roster_collection,
    group: str = "",
    name: str = None,
    address: str = None,
    city: str = None,
    state: str = None,
    debug: bool = False,
):
    # initialize return values
    member_id: str = "0"
    score: float = 0.0

    if group == "MISSING CONTRACT":
        return member_id, score

    aggregation = BUILD_AGGREGATION(
        group=group, name=name, address=address, city=city, state=state
    )

    result = list(collection.aggregate(aggregation))

    if result:
        doc = result[0]

        member_id = doc["member_id"]
        score = doc["score"]

        if debug:
            print()
            print(f"{group} {name} {address} {city} {state}")
            print(f"{member_id} {score}")
            print(doc)
            print()

    return member_id, score


@lru_cache(maxsize=None)
def find_item_in_database(item: str) -> dict:
    collection = gc_rbt(SCHED_DATA)

    result = collection.find_one({"part": item})

    return result


@lru_cache(maxsize=None)
def find_item_and_convert_uom(item: str, uom: str, qty: int) -> float:
    item_dict = find_item_in_database(item)

    if item_dict is None:
        return 0.0

    return CONVERT_UOM(item_dict, uom, qty)


@lru_cache(maxsize=None)
def add_license(gpo: str, name: str, address: str, city: str, state: str) -> str:
    gpo = "MEDASSETS" if gpo in VHA_VIZIENT_MEDASSETS else gpo

    name = FIX_NAME(name)
    name = name.lower().strip()
    address = address.strip().lower().strip()
    city = city.strip().lower().strip()
    state = state.strip().lower().strip()

    assert len(gpo) > 0, "gpo is required"
    assert len(name) > 0, "name is required"

    lic, score = find_license(
        group=gpo, name=name, address=address, city=city, state=state
    )

    return f"{lic}|{score}"


current_contracts = get_current_contracts()


@lru_cache(maxsize=None)
def add_gpo_to_df(contract: str) -> str:
    contract = contract.upper().strip()
    return current_contracts.get(contract, "MISSING CONTRACT").upper()


def build_df_from_warehouse_using_fields_file(fields_file: str) -> pd.DataFrame:
    data_warehouse_collection = gc_rbt(DATA_WAREHOUSE)

    hidden = "hidden"
    gpo = "GPO"
    check = "CHECK_LICENSE"
    lic = "LICENSE"
    score = "SCORE"
    cs_conv = "SHIP QTY AS CS"

    kwargs = get_field_file_body_and_decode_kwargs(prefix="input/", key=fields_file)

    print(kwargs)

    # initialize variables for dataframe from fields_file

    filter = kwargs.get("filter", None)
    period = kwargs.get("period", None)

    assert filter is not None, "filter is required"
    assert period is not None, "period is required"

    contract_map = kwargs.get("contract_map", None)
    skip_license = kwargs.get("skip_license", False)

    contract = kwargs.get("contract")
    claim_nbr = kwargs.get("claim_nbr")
    order_nbr = kwargs.get("order_nbr")
    invoice_nbr = kwargs.get("invoice_nbr")
    invoice_date = kwargs.get("invoice_date")
    part = kwargs.get("part")
    ship_qty = kwargs.get("ship_qty")
    unit_rebate = kwargs.get("unit_rebate", None)
    rebate = kwargs.get("rebate")
    name = kwargs.get("name")
    addr = kwargs.get("addr")
    city = kwargs.get("city")
    state = kwargs.get("state")
    postal = kwargs.get("postal", "0")
    uom = kwargs.get("uom", None)
    cost = kwargs.get("cost")
    addr1 = kwargs.get("addr1", None)
    addr2 = kwargs.get("addr2", None)
    cost_calculation = kwargs.get("cost_calculation", None)
    part_regex = kwargs.get("part_regex", None)
    cull_missing_contracts = kwargs.get("cull_missing_contracts", False)
    uom_regex = kwargs.get("uom_regex", None)

    added_to_queue = "added_to_queue"

    # /initialize variables for dataframe from fields_file

    # initialize columns for dataframe

    orig_cols, output_cols = SET_COLUMNS(
        hidden=hidden,
        gpo=gpo,
        check=check,
        lic=lic,
        score=score,
        cs_conv=cs_conv,
        contract=contract,
        claim_nbr=claim_nbr,
        order_nbr=order_nbr,
        invoice_nbr=invoice_nbr,
        invoice_date=invoice_date,
        part=part,
        ship_qty=ship_qty,
        unit_rebate=unit_rebate,
        rebate=rebate,
        name=name,
        addr=addr,
        city=city,
        state=state,
        postal=postal,
        uom=uom,
        cost=cost,
        added_to_queue=added_to_queue,
    )

    # /initialize columns for dataframe

    # initialize dataframe

    print(filter)

    datawarehouse = get_documents(data_warehouse_collection, filter)

    print(datawarehouse)

    df = pd.DataFrame(datawarehouse)

    # /initialize dataframe

    print(df)

    # clean df

    df[hidden] = df.apply(lambda _: period, axis=1)

    df = df[df[name].notna()].copy()

    if contract_map:
        df[contract] = df[contract].apply(lambda x: contract_map.get(x, x))

    if addr is not None:
        df[addr] = df[addr].apply(lambda x: x.lower().lstrip('="').rstrip('"').strip())

    if addr1 and addr2:
        df[addr1] = df[addr1].apply(
            lambda x: str(x).lower().lstrip('="').rstrip('"').strip()
            if isinstance(x, str)
            else ""
        )
        df[addr2] = df[addr2].apply(
            lambda x: str(x).lower().lstrip('="').rstrip('"').strip()
            if isinstance(x, str)
            else ""
        )

        df[addr] = df.apply(lambda x: FIX_ADDRESS(x[addr1], x[addr2]), axis=1)

    if uom_regex is not None:
        df[uom] = df[uom].apply(lambda x: re.sub(uom_regex, "", x).strip())
    else:
        if uom:
            df[uom] = df[uom].apply(
                lambda x: "CA"
                if re.search(
                    r"(\d+x|\d+\/)",
                    x.upper().lstrip('="').rstrip('"').strip(),
                    re.IGNORECASE,
                )
                else x.upper().lstrip('="').rstrip('"').strip()
            )
        else:
            uom = "uom"
            orig_cols[17] = uom
            df[uom] = df.apply(lambda _: "CA", axis=1)

    df[invoice_date] = df[invoice_date].apply(
        lambda x: x.lower().lstrip('="').rstrip('"').strip()
        if isinstance(x, str)
        else str(x)
    )

    df[added_to_queue] = datetime.now()

    df[name] = df[name].apply(lambda x: x.lower().lstrip('="').rstrip('"').strip())

    df[city] = df[city].apply(lambda x: x.lower().lstrip('="').rstrip('"').strip())

    df[cost] = df[cost].apply(
        lambda x: x.strip().lower().lstrip('="$').rstrip('"')
        if isinstance(x, str)
        else x
    )

    df[state] = df[state].apply(lambda x: x.strip().lower().lstrip('="').rstrip('"'))
    df[claim_nbr] = df[claim_nbr].apply(
        lambda x: str(x).rstrip(".0").lower().lstrip('="').rstrip('"').strip()
    )

    df[[cost, ship_qty, rebate]] = df[[cost, ship_qty, rebate]].apply(pd.to_numeric)

    try:
        df[invoice_date] = df[invoice_date].apply(pd.to_datetime)
    except:
        try:
            df[invoice_date] = df[invoice_date].apply(
                lambda x: pd.to_datetime(x, format="%m/%d/%Y")
            )
        except:
            try:
                df[invoice_date] = df[invoice_date].apply(
                    lambda x: pd.to_datetime(x, format="%y%m%d")
                )
            except:
                try:
                    df[invoice_date] = df[invoice_date].apply(
                        lambda x: pd.to_datetime(x, format="%m%d%Y")
                    )
                except:
                    print("Could not convert invoice_date to datetime")

    if part_regex:
        df[part] = df[part].apply(
            lambda x: re.sub(
                pattern=part_regex,
                repl="",
                string=str(x)
                .lstrip('="')
                .rstrip('"')
                .lstrip("0")
                .replace(".0", "")
                .strip()
                .lower(),
                flags=re.IGNORECASE,
            ).upper()
        )
    else:
        df[part] = df[part].apply(
            lambda x: str(x)
            .replace(".0", "")
            .lower()
            .lstrip('="')
            .rstrip('"')
            .strip()
            .upper()
        )

    df[order_nbr] = df[order_nbr].apply(
        lambda x: str(x).rstrip(".0").lower().lstrip('="').rstrip('"').strip()
    )
    df[invoice_nbr] = df[invoice_nbr].apply(
        lambda x: str(x).rstrip(".0").lower().lstrip('="').rstrip('"').strip()
    )
    df[contract] = df[contract].apply(
        lambda x: x.lstrip('="').rstrip('"').strip() if isinstance(x, str) else ""
    )

    if unit_rebate:
        df[unit_rebate] = df[unit_rebate].apply(pd.to_numeric)
    else:
        unit_rebate = "unit_rebate"
        orig_cols[15] = unit_rebate
        df[unit_rebate] = df.apply(lambda x: x[rebate] / x[ship_qty], axis=1)

    if cost_calculation == "cost - rebate * ship_qty":
        df[cost] = df.apply(lambda x: (x[cost] - x[unit_rebate]) * x[ship_qty], axis=1)
    elif cost_calculation == "cost * ship_qty":
        df[cost] = df.apply(lambda x: x[cost] * x[ship_qty], axis=1)

    # /clean df

    # add gpo

    print("add_gpo() >\t", add_gpo_to_df.cache_info())
    df[gpo] = df.apply(lambda x: add_gpo_to_df(x[contract]), axis=1)
    print("add_gpo() >\t", add_gpo_to_df.cache_info())

    if cull_missing_contracts:
        df = df[df[contract] != ""].copy()

    # /add gpo

    # add license and search score and a check column

    if skip_license:
        df[lic] = ""
        df[score] = 99
        df[check] = False
    else:
        print("add_license() >\t", add_license.cache_info())
        df["temp"] = df.apply(
            lambda x: add_license(x[gpo], x[name], x[addr], x[city], x[state]),
            axis=1,
        )
        print("add_license() >\t", add_license.cache_info())

        df[lic] = df.apply(lambda x: str(x["temp"].split("|")[0]), axis=1)
        df[score] = df.apply(lambda x: float(x["temp"].split("|")[1]), axis=1)

        # calculate confidence minimum
        confidence_min = df[score].mean() * 0.85
        df[check] = df.apply(lambda x: x[score] <= confidence_min, axis=1)

    # /add license and search score and a check column

    # convert uom

    print("find_item_and_convert_uom() >\t", find_item_and_convert_uom.cache_info())
    df[cs_conv] = df.apply(
        lambda x: find_item_and_convert_uom(
            str(x[part]).lstrip("0"), x[uom], x[ship_qty]
        ),
        axis=1,
    )
    print("find_item_and_convert_uom() >\t", find_item_and_convert_uom.cache_info())

    # /convert uom

    df = df[orig_cols].sort_values(by=[lic, contract, part])

    df.columns = output_cols

    return df
