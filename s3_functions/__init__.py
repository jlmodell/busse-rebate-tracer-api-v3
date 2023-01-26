import json
import os
from datetime import datetime
from typing import Optional

from s3_storage import CLIENT, S3_BUCKET

from .field_file import *
from .savers import *


def get_field_file_body_and_decode_kwargs(prefix: str, key: str) -> dict:
    field_file = (
        CLIENT.get_object(Bucket=S3_BUCKET, Key=prefix + key)["Body"]
        .read()
        .decode("utf-8")
    )

    kwargs = json.loads(field_file)

    month = kwargs.get("month").upper()
    year = kwargs.get("year").upper()

    kwargs["period"] = f"{month}{year}-" + kwargs.get("period")

    return kwargs


def update_field_file_body_and_save(
    prefix: str,
    key: str,
    filter: dict,
    month: str,
    year: str,
    period: Optional[str] = None,
) -> None:
    field_file = (
        CLIENT.get_object(Bucket=S3_BUCKET, Key=prefix + key)["Body"]
        .read()
        .decode("utf-8")
    )

    kwargs = json.loads(field_file)

    kwargs["filter"] = filter
    try:
        kwargs["file"] = os.path.basename(filter.get("__file__"))
    except TypeError:
        pass
    kwargs["month"] = month
    kwargs["year"] = year

    if period:
        kwargs["period"] = period

    CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}backups/{datetime.now():%Y%m%d%H%M%S}_{key}",
        Body=json.dumps(kwargs),
        ContentType="application/json",
    )

    new_field_file = json.dumps(kwargs)

    CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=prefix + key,
        Body=new_field_file,
        ContentType="application/json",
    )


def save_df_to_s3_as_excel(df: pd.DataFrame, prefix: str, filename: str):
    data = GET_BYTES(df, filename)

    CLIENT.put_object(Bucket=S3_BUCKET, Key=prefix + filename, Body=data)


def save_tracings_df_as_html_with_javascript_css(
    df: pd.DataFrame, prefix: str, filename: str
):
    data = GET_HTML_WITH_JS_CSS(df)

    CLIENT.put_object(Bucket=S3_BUCKET, Key=prefix + filename, Body=data)


def move_file_to_completed_folder(prefix: str, storage_key: str, success_key: str):
    CLIENT.copy_object(
        Bucket=S3_BUCKET,
        CopySource={"Bucket": S3_BUCKET, "Key": prefix + storage_key},
        Key=prefix + success_key,
    )

    CLIENT.delete_object(Bucket=S3_BUCKET, Key=prefix + storage_key)
