import pandas as pd
from math import floor
from typing import Tuple

from constants import LIST_OF_DISTINCT_PARTS


def CLEANUP_TRACINGS_DF(df: pd.DataFrame) -> pd.DataFrame:
    df["gpo"] = df.apply(
        lambda x: x["gpo"] if x["gpo"] != "MISSING CONTRACT" else x["contract"],
        axis=1,
    )

    df = df.loc[df["part"].isin(LIST_OF_DISTINCT_PARTS), :].copy()

    df["each_size"] = df["part_data"].apply(lambda x: x[0].get("each_per_case", 0))

    df["unit_rebate_as_cs"] = df.apply(
        lambda x: x.get("rebate") / x.get("ship_qty_as_cs")
        if x.get("ship_qty_as_cs") != 0
        else 0.0,
        axis=1,
    )

    df["ship_qty_as_cs_whole"] = df["ship_qty_as_cs"].apply(lambda x: floor(x))

    df["ship_qty_as_cs_partial"] = df.apply(
        lambda x: x["ship_qty_as_cs"] - x["ship_qty_as_cs_whole"], axis=1
    )

    df["rebate_whole"] = df.apply(
        lambda x: x["ship_qty_as_cs_whole"] * x["unit_rebate_as_cs"], axis=1
    )

    df["rebate_partial"] = df.apply(
        lambda x: x["ship_qty_as_cs_partial"] * x["unit_rebate_as_cs"], axis=1
    )

    df = df[df["rebate"] != 0].copy()

    return df


def TRANSFORM_BY_DISTRIBUTOR_BY_CONTRACT_BY_PART(df: pd.DataFrame) -> Tuple:
    items = {
        x["part_data"][0].get("part"): x["part_data"][0].get("each_per_case")
        for x in df.to_dict("records")
    }

    df = pd.DataFrame(
        df[df["contract"] != ""]
        .groupby(
            [
                "period",
                "contract",
                "part",
                "uom",
                "contract_price",
                "contract_name",
                "contract_end",
            ],
            as_index=False,
        )  # added "uom" removed "each_size"
        .sum(["ship_qty", "ship_qty_as_cs", "rebate", "cost"])
    )

    df["unit_rebate_as_cs"] = df.apply(
        lambda x: x.get("rebate") / x.get("ship_qty_as_cs")
        if x.get("ship_qty_as_cs") != 0
        else 0.0,
        axis=1,
    )

    df["ship_qty_as_cs_whole"] = df["ship_qty_as_cs"].apply(lambda x: floor(x))

    df["ship_qty_as_cs_partial"] = df.apply(
        lambda x: x["ship_qty_as_cs"] - x["ship_qty_as_cs_whole"], axis=1
    )

    df["rebate_whole"] = df.apply(
        lambda x: x["ship_qty_as_cs_whole"] * x["unit_rebate_as_cs"], axis=1
    )

    df["rebate_partial"] = df.apply(
        lambda x: x["ship_qty_as_cs_partial"] * x["unit_rebate_as_cs"], axis=1
    )

    df["each_size"] = df["part"].apply(lambda x: items.get(x))

    df = df[
        [
            "period",
            # "name",
            # "addr",
            # "city",
            # "state",
            # "gpo",
            "each_size",
            "contract",
            "contract_name",
            "contract_end",
            "part",
            "contract_price",
            "ship_qty",  # added
            "uom",  # added
            # "license",
            "ship_qty_as_cs_whole",
            "rebate_whole",
            "ship_qty_as_cs_partial",
            "rebate_partial",
            "ship_qty_as_cs",
            "rebate",
            "cost",
        ]
    ].copy()

    unique_periods = df["period"].unique()

    df_with_summary = pd.DataFrame()

    for period in unique_periods:
        df_temp = df.loc[df["period"] == period, :].copy()

        df_temp_sum_row = pd.DataFrame(
            data=df_temp[
                [
                    "ship_qty",  # added
                    "ship_qty_as_cs_whole",
                    "rebate_whole",
                    "ship_qty_as_cs_partial",
                    "rebate_partial",
                    "ship_qty_as_cs",
                    "rebate",
                    "cost",
                ]
            ].sum()
        ).T

        df_temp_sum_row["period"] = period

        df_temp_sum_row = df_temp_sum_row.reindex(columns=df_temp.columns)

        df_temp = pd.concat([df_temp, df_temp_sum_row], axis=0)

        df_with_summary = pd.concat([df_with_summary, df_temp], axis=0)

    return df, df_with_summary
