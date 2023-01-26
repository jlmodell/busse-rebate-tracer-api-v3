import io
import os
import re

import numpy as np
import pandas as pd

from s3_storage import CLIENT, S3_BUCKET

STR_FIELDS = r"(PART|CAT|MATERIAL|ITEM|INVOICE|ORDER|post(al)|zip|zip(code| code))"


def GET_DTYPES(file_path: str, delimiter: str = ",", header_row: int = 0) -> dict:
    if re.search(r".xl(sx|sm|s)$", file_path, re.IGNORECASE):
        dtypes = dict(
            pd.read_excel(
                file_path,
                header=header_row if header_row != -1 else None,
            ).dtypes
        )
    elif re.search(r".(csv|txt)$", file_path, re.IGNORECASE):
        dtypes = dict(
            pd.read_csv(
                file_path,
                delimiter=delimiter,
                header=header_row if header_row != -1 else None,
            ).dtypes,
        )

    try:
        for key in dtypes.keys():
            if re.search(STR_FIELDS, key, re.IGNORECASE):
                dtypes[key] = "str"
    except Exception as e:
        print(e)

    return dtypes


def GET_DTYPES_S3(
    prefix: str, key: str, delimiter: str = ",", header_row: int = 0
) -> dict:
    dtypes = dict()

    if re.search(r".xl(sx|sm|s)$", key, re.IGNORECASE):
        dtypes = dict(
            pd.read_excel(
                io.BytesIO(
                    CLIENT.get_object(Bucket=S3_BUCKET, Key=prefix + key)["Body"].read()
                ),
                header=header_row if header_row != -1 else None,
            ).dtypes
        )
    elif re.search(r".(csv|txt)$", key, re.IGNORECASE):
        dtypes = dict(
            pd.read_csv(
                io.BytesIO(
                    CLIENT.get_object(Bucket=S3_BUCKET, Key=prefix + key)["Body"].read()
                ),
                delimiter=delimiter,
                header=header_row if header_row != -1 else None,
            ).dtypes,
        )

    try:
        for key in dtypes.keys():
            if re.search(STR_FIELDS, key, re.IGNORECASE):
                dtypes[key] = "str"
    except Exception as e:
        print(e)

    return dtypes


def GET_CLEAN_DF_TO_INGEST(
    df: pd.DataFrame, file_path: str, month: str, year: str
) -> pd.DataFrame:
    df.fillna("")
    df = df[df[df.columns[0]].notnull()].copy()
    df = df[df[df.columns[0]] != ""].copy()
    try:
        df = df[df["UOM"].notnull()]
    except Exception:
        pass

    cols = df.columns

    for col in cols:
        try:
            df[col] = df[col].str.strip()
            if col == "PART NBR":
                df[col] = df[col].str.lstrip("0")

        except Exception:
            pass

    df = df.replace(np.nan, "", regex=True).copy()

    try:
        df["__file__"] = os.path.basename(file_path)
    except TypeError:
        df["__file__"] = file_path.split("/")[-1]
    df["__year__"] = year
    df["__month__"] = month

    df.columns = [str(x) for x in df.columns]

    return df

    return df
