import io
import pandas as pd
from s3_storage import CLIENT, S3_BUCKET


def ingest_from_s3(prefix: str, key: str) -> pd.DataFrame:
    df = pd.DataFrame()

    try:
        df = pd.read_excel(
            io.BytesIO(
                CLIENT.get_object(Bucket=S3_BUCKET, Key=prefix + key)["Body"].read()
            )
        )
    except Exception as e:
        print(e)
        print(S3_BUCKET)
        print(prefix + key)

    return df


def get_list_of_files(prefix: str) -> list:
    response = CLIENT.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    files = response.get("Contents", [])
    if len(files) > 0:
        return [file["Key"] for file in files]

    return []


if __name__ == "__main__":
    df = ingest_from_s3("rebate_trace_files/", "^MGM*.xlsx")
    print(df)
