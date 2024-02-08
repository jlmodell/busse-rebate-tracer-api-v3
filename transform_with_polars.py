import functools
import re
import time
from datetime import datetime
from functools import lru_cache

import pandas as pd
import polars as pl
from pymongo.collection import Collection
from rich import print

from constants import DATA_WAREHOUSE, ROSTERS, SCHED_DATA, VHA_VIZIENT_MEDASSETS
from database import current_contracts, gc_rbt, get_documents
from s3_functions import SET_COLUMNS, get_field_file_body_and_decode_kwargs
from transformers import BUILD_AGGREGATION, CONVERT_UOM, FIX_ADDRESS, FIX_NAME

roster_collection = gc_rbt(ROSTERS)

print(len(current_contracts))


def time_execution(func):
    """
    Decorator to time the execution of a function and print the elapsed time.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start timing
        result = func(*args, **kwargs)  # Execute the function
        end_time = time.time()  # End timing
        elapsed_time = end_time - start_time  # Calculate elapsed time
        print(f"Function {func.__name__} took {elapsed_time:.6f} seconds to execute.")
        return result

    return wrapper


@lru_cache(maxsize=1024)
def gpo_search(contract):
    global current_contracts

    # Check if the exact GPO is in the dictionary
    if contract in current_contracts:
        return current_contracts[contract].upper()

    # Use regex to find a similar GPO
    contract_regex = re.compile(rf"^{contract[:-2]}")
    current_contracts_list = list(current_contracts.keys())

    found = list(filter(contract_regex.match, current_contracts_list))

    if found:
        return current_contracts[found[0]].upper()

    return "MISSING CONTRACT"


@lru_cache(maxsize=1024)
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

    if group not in [
        "APTITUDE",
        "HEALTHTRUST",
        "HPG",
        "LIJ",
        "MAGNET",
        "MEDASSETS",
        "MEDIGROUP",
        "MHA",
        "PREMIER",
        "SCMA",
        "TRG",
        "UNITY",
    ]:
        return member_id, score

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


@lru_cache(maxsize=1024)
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


licenses = {}


@lru_cache(maxsize=1024)
def find_license_without_score(
    gpo: str, name: str, address: str, city: str, state: str
) -> str:
    gpo = "MEDASSETS" if gpo in VHA_VIZIENT_MEDASSETS else gpo

    name = FIX_NAME(name)
    name = name.lower().strip()
    address = address.strip().lower().strip()
    city = city.strip().lower().strip()
    state = state.strip().lower().strip()

    if not gpo:
        raise ValueError("gpo is required")
    if not name:
        raise ValueError("name is required")

    lic, score = find_license(
        group=gpo, name=name, address=address, city=city, state=state
    )

    licenses[(gpo, name)] = (lic, score)

    print(f"{gpo} {name} {lic} {score}")

    return lic


@lru_cache(maxsize=1024)
def find_score(gpo: str, name: str) -> float:
    gpo = "MEDASSETS" if gpo in VHA_VIZIENT_MEDASSETS else gpo

    name = FIX_NAME(name)
    name = name.lower().strip()

    if not gpo:
        raise ValueError("gpo is required")
    if not name:
        raise ValueError("name is required")

    return licenses.get((gpo, name), ("", 0.0))[1]


# def vectorized_license_search(
#     gpo_series, name_series, addr_series, city_series, state_series
# ):
#     result = [
#         find_license_without_score(gpo, name, addr, city, state)
#         for gpo, name, addr, city, state in zip(
#             gpo_series, name_series, addr_series, city_series, state_series
#         )
#     ]

#     return pl.Series(result)


# def vectorized_score_search(gpo_series, name_series):
#     result = [find_score(gpo, name) for gpo, name in zip(gpo_series, name_series)]

#     return pl.Series(result)


# def vectorized_score_analysis(score_series):
#     result = [score <= score_series.mean() * 0.85 for score in score_series]

#     return pl.Series(result)


@time_execution
def build_df_from_warehouse_using_fields_file_using_polars(
    fields_file: str, debug: bool = False
) -> pd.DataFrame:
    data_warehouse_collection = gc_rbt(DATA_WAREHOUSE)
    # tracings_collection = gc_rbt(TRACINGS)

    hidden = "hidden"
    gpo = "GPO"
    check = "CHECK_LICENSE"
    lic = "LICENSE"
    score = "SCORE"
    cs_conv = "SHIP QTY AS CS"

    ff_kwargs = get_field_file_body_and_decode_kwargs(prefix="input/", key=fields_file)

    if debug:
        print(ff_kwargs)

    # initialize variables for dataframe from fields_file

    filter = ff_kwargs.get("filter", None)
    period = ff_kwargs.get("period", None)

    assert filter is not None, "filter is required"
    assert period is not None, "period is required"

    contract_map = ff_kwargs.get("contract_map", None)
    # skip_license = ff_kwargs.get("skip_license", False)

    contract = ff_kwargs.get("contract")
    claim_nbr = ff_kwargs.get("claim_nbr")
    order_nbr = ff_kwargs.get("order_nbr")
    invoice_nbr = ff_kwargs.get("invoice_nbr")
    invoice_date = ff_kwargs.get("invoice_date")
    part = ff_kwargs.get("part")
    ship_qty = ff_kwargs.get("ship_qty")
    unit_rebate = ff_kwargs.get("unit_rebate", None)
    rebate = ff_kwargs.get("rebate")
    name = ff_kwargs.get("name")
    addr = ff_kwargs.get("addr")
    city = ff_kwargs.get("city")
    state = ff_kwargs.get("state")
    postal = ff_kwargs.get("postal", "0")
    uom = ff_kwargs.get("uom", None)
    cost = ff_kwargs.get("cost")
    addr1 = ff_kwargs.get("addr1", None)
    addr2 = ff_kwargs.get("addr2", None)
    cost_calculation = ff_kwargs.get("cost_calculation", None)
    part_regex = ff_kwargs.get("part_regex", None)
    cull_missing_contracts = ff_kwargs.get("cull_missing_contracts", False)
    uom_regex = ff_kwargs.get("uom_regex", None)

    added_to_queue = "added_to_queue"

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

    datawarehouse = get_documents(data_warehouse_collection, filter)

    if debug:
        print(len(datawarehouse))
        print(datawarehouse[:5])

    df = pd.DataFrame(datawarehouse)

    # filter out rows where name is null

    df = df[df[name].notna()].copy()

    # convert pandas dataframe to polars dataframe

    df = pl.DataFrame(df)

    # add literals to dataframe and cleanup columns in use

    def vectorized_gpo_search(contract_series):
        result = [gpo_search(contract) for contract in contract_series]

        return pl.Series(result)

    def vectorized_license_search(
        gpo_expr, name_expr, addr_expr, city_expr, state_expr
    ):
        result = [
            find_license_without_score(gpo, name, addr, city, state)
            for gpo, name, addr, city, state in zip(
                gpo_expr, name_expr, addr_expr, city_expr, state_expr
            )
        ]

        return pl.Series(result)

    def vectorized_score_search(gpo_expr, name_expr):
        result = [find_score(gpo, name) for gpo, name in zip(gpo_expr, name_expr)]

        return pl.Series(result)

    def vectorized_addr_cleanup(addr1_series, addr2_series) -> str:
        result = [
            FIX_ADDRESS(addr1, addr2)
            for addr1, addr2 in zip(addr1_series, addr2_series)
        ]

        return pl.Series(result)

    def vectorized_cost_calculation_1(cost_series, unit_rebate_series, ship_qty_series):
        result = [
            (cost - unit_rebate) * ship_qty
            for cost, unit_rebate, ship_qty in zip(
                cost_series, unit_rebate_series, ship_qty_series
            )
        ]

        return pl.Series(result)

    def vectorized_cost_calculation_2(cost_series, ship_qty_series):
        result = [
            cost * ship_qty for cost, ship_qty in zip(cost_series, ship_qty_series)
        ]

        return pl.Series(result)

    def vectorized_uom_to_cs_conversion(part_series, uom_series, ship_qty_series):
        result = [
            find_item_and_convert_uom(part, uom, ship_qty)
            for part, uom, ship_qty in zip(part_series, uom_series, ship_qty_series)
        ]

        return pl.Series(result)

    if addr1 and addr2:
        df = df.with_columns(
            df[addr1]
            .map_elements(lambda addr1: addr1.lower().lstrip('="').rstrip('"').strip())
            .alias(addr1),
            df[addr2]
            .map_elements(lambda addr2: addr2.lower().lstrip('="').rstrip('"').strip())
            .alias(addr2),
        ).with_columns(
            vectorized_addr_cleanup(df[addr1], df[addr2]).alias(addr),
        )
    else:
        df = df.with_columns(
            df[addr]
            .map_elements(lambda addr: addr.lower().lstrip('="').rstrip('"').strip())
            .alias(addr),
        )

    if part_regex:
        df = df.with_columns(
            df[part]
            .map_elements(
                lambda p: re.sub(
                    pattern=part_regex,
                    repl="",
                    string=str(p)
                    .lstrip('="')
                    .rstrip('"')
                    .lstrip("0")
                    .replace(".0", "")
                    .strip()
                    .lower(),
                    flags=re.IGNORECASE,
                ).upper()
            )
            .alias(part)
        )
    else:
        df = df.with_columns(
            df[part]
            .map_elements(
                lambda p: str(p)
                .replace(".0", "")
                .lower()
                .lstrip('="')
                .rstrip('"')
                .strip()
                .upper()
            )
            .alias(part)
        )

    if uom_regex:
        df = df.with_columns(
            df[uom].map_elements(
                lambda uom_str: re.sub(
                    uom_regex, "", uom_str.upper().lstrip('="').rstrip('"').strip()
                ).strip()
            )
        ).alias(uom)
    else:
        df = df.with_columns(
            df[uom]
            .map_elements(
                lambda uom_str: "CA"
                if re.search(
                    r"(\d+x|\d+\/)",
                    uom_str.upper().lstrip('="').rstrip('"').strip(),
                    re.IGNORECASE,
                )
                else uom_str.upper().lstrip('="').rstrip('"').strip()
            )
            .alias(uom)
        )

    if contract_map:
        df = df.with_columns(
            df[contract]
            .map_elements(lambda contract: contract_map.get(contract, contract))
            .alias(contract)
        )
    else:
        df = df.with_columns(
            df[contract]
            .map_elements(lambda contract: contract.lstrip('="').rstrip('"').strip())
            .alias(contract)
        )

    if claim_nbr:
        df = df.with_columns(
            df[claim_nbr]
            .map_elements(
                lambda claim_nbr: claim_nbr.lower().lstrip('="').rstrip('"').strip()
            )
            .alias(claim_nbr)
        )

    if order_nbr:
        df = df.with_columns(
            df[order_nbr]
            .map_elements(
                lambda order_nbr: order_nbr.lower().lstrip('="').rstrip('"').strip()
            )
            .alias(order_nbr)
        )

    if cost_calculation:
        df = df.with_columns(
            df[cost].map_elements(
                lambda cost: pd.to_numeric(
                    cost.strip().lower().lstrip('="$').rstrip('"').replace(",", "")
                )
                if isinstance(cost, str)
                else pd.to_numeric(cost)
            ),
        ).with_columns(
            pl.when(cost_calculation == "cost - rebate * ship_qty")
            .then(
                vectorized_cost_calculation_1(df[cost], df[unit_rebate], df[ship_qty])
            )
            .otherwise(vectorized_cost_calculation_2(df[cost], df[ship_qty]))
            .alias(cost),
        )
    else:
        df = df.with_columns(
            df[cost].map_elements(
                lambda cost: pd.to_numeric(
                    cost.strip().lower().lstrip('="$').rstrip('"').replace(",", "")
                )
                if isinstance(cost, str)
                else pd.to_numeric(cost)
            ),
        )

    if debug:
        print("step 1 good", df.head())

    df = df.with_columns(
        df[invoice_date]
        .map_elements(
            lambda inv_date: inv_date.lower().lstrip('="').rstrip('"').strip()
            if isinstance(inv_date, str)
            else str(inv_date)
        )
        .alias(invoice_date),
    )

    if debug:
        print("step 2 good", df.head())

    df = (
        df.lazy()
        .with_columns(
            # period
            pl.lit(period).alias(hidden),
            # added_to_queue
            pl.lit(datetime.now()).alias(added_to_queue),
            # name
            pl.col(name)
            .map_elements(lambda name: name.lower().lstrip('="').rstrip('"').strip())
            .alias(name),
            # city
            pl.col(city)
            .map_elements(lambda city: city.lower().lstrip('="').rstrip('"').strip())
            .alias(city),
            # state
            pl.col(state)
            .map_elements(lambda state: state.lower().lstrip('="').rstrip('"').strip())
            .alias(state),
            # postal
            pl.col(postal)
            .map_elements(
                lambda postal: postal.lower().lstrip('="').rstrip('"').strip()
            )
            .alias(postal),
            # invoice_nbr
            pl.col(invoice_nbr)
            .map_elements(
                lambda x: str(x).rstrip(".0").lower().lstrip('="').rstrip('"').strip()
            )
            .alias(invoice_nbr),
            # ship_qty
            pl.col(ship_qty).map_elements(
                lambda ship_qty: pd.to_numeric(
                    ship_qty.strip().lower().lstrip('="').rstrip('"').replace(",", "")
                )
                if isinstance(ship_qty, str)
                else pd.to_numeric(ship_qty)
            ),
            # rebate
            pl.col(rebate).map_elements(
                lambda rebate: pd.to_numeric(
                    rebate.strip().lower().lstrip('="$').rstrip('"').replace(",", "")
                )
                if isinstance(rebate, str)
                else pd.to_numeric(rebate)
            ),
        )
        .with_columns(
            pl.when(pl.col(unit_rebate).is_not_null())
            .then(pl.col(unit_rebate).cast(pl.Float32))
            .otherwise(
                # vectorized_unit_rebate_calculation(pl.col(rebate), pl.col(ship_qty))
                pl.col(rebate) / pl.col(ship_qty)
            )
            .alias(unit_rebate),
            # pl.col(contract)
            # .map_elements(
            #     lambda contract: current_contracts.get(
            #         contract, "MISSING CONTRACT"
            #     ).upper()
            # )
            # .alias(gpo),
        )
        .collect()
    )

    # apply cleanup functions to columns
    if debug:
        print("step 3 good", df.head())

    df = df.with_columns(
        vectorized_uom_to_cs_conversion(df[part], df[uom], df[ship_qty]).alias(cs_conv),
        vectorized_gpo_search(df[contract]).alias(gpo),
    )

    if debug:
        print("step 4 good", df.head())

    # filter out rows where contracts are missing

    if cull_missing_contracts:
        df = df.filter(pl.col(contract) != "MISSING CONTRACT")

        # find licenses

    # print(df.columns)
    # print(type(df))

    df = df.with_columns(
        vectorized_license_search(
            df[gpo], df[name], df[addr], df[city], df[state]
        ).alias(lic),
    )

    df = df.with_columns(
        vectorized_score_search(df[gpo], df[name]).alias(score),
    )

    if debug:
        print("step 5A good", df.head())

    mean_score = df[score].mean() * 0.85

    # print("mean_score", mean_score)

    df = df.with_columns(
        (pl.col(score) < mean_score).alias(check),
    )

    if debug:
        print("step 5B good", df.head())

    # def handle_duplicate_columns_in_list_and_df(col_names, df):
    #     # Create a dictionary to keep track of the counts of each column name
    #     col_counts = {}
    #     new_cols = []

    #     for col in col_names:
    #         if col in col_counts:
    #             # Increment the count for this column name
    #             col_counts[col] += 1
    #             # Create a new column name with a suffix based on the count
    #             new_col_name = f"{col}_{col_counts[col]}"
    #             # Check if the new column name already exists in the DataFrame
    #             if new_col_name in df.columns:
    #                 raise ValueError(
    #                     f"Column name {new_col_name} already exists in the DataFrame."
    #                 )
    #             # Prepare the new column for addition, duplicating the original column
    #             new_cols.append(df[col].alias(new_col_name))
    #             # Update the column name in the list
    #             col_names[col_names.index(col)] = new_col_name
    #         else:
    #             # Initialize the count for this column name
    #             col_counts[col] = 1

    #     # Add the new columns to the DataFrame
    #     if new_cols:
    #         df = df.with_columns(new_cols)

    #     return col_names, df

    # orig_cols, df = handle_duplicate_columns_in_list_and_df(orig_cols, df)

    # df = df.select(orig_cols)

    # df = df.rename(dict(zip(df.columns, output_cols)))

    if debug:
        print("step 6 good", df.head())

    # convert back to pandas dataframe

    df = df.to_pandas()
    df = df[orig_cols].copy()
    df.sort_values(by=[gpo, lic, part], inplace=True)
    df.columns = output_cols

    if debug:
        print("step 7 good", df.head())

    df.to_excel("polars.xlsx", index=False)

    return df


if __name__ == "__main__":
    df = build_df_from_warehouse_using_fields_file_using_polars(
        fields_file="concordance.json",
        debug=True,
    )

    # df2 = build_df_from_warehouse_using_fields_file(
    #     fields_file="concordance.json",
    # )
