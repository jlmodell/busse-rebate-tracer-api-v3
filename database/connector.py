from pymongo import MongoClient

from constants import BUSSE_REBATE_TRACES, BUSSE_REBATE_TRACES_COLLECTIONS, BUSSE_PRICING, BUSSE_PRICING_COLLECTIONS, MONGODB_URI, DATABASES

from pymongo.collection import Collection
from pymongo.database import Database


def GET_CLIENT() -> MongoClient:
    return MongoClient(MONGODB_URI)


def GET_DATABASE(client: MongoClient, DATABASE: str) -> Database:
    __db__ = DATABASE.upper().strip()

    if __db__ == BUSSE_REBATE_TRACES:
        assert __db__ in DATABASES, f"{DATABASE} not in the databases list, check database_constants.py"

        return client[DATABASES[__db__]]

    if __db__ == BUSSE_PRICING:
        assert __db__ in DATABASES, f"{DATABASE} not in the databases list, check database_constants.py"

        return client[DATABASES[__db__]]

    return None


def GET_COLLECTION(db: Database, collection_key: str) -> Collection:
    __db_name__ = db.name

    if __db_name__ == DATABASES[BUSSE_REBATE_TRACES]:
        return db[BUSSE_REBATE_TRACES_COLLECTIONS[collection_key]]

    if __db_name__ == DATABASES[BUSSE_PRICING]:
        return db[BUSSE_PRICING_COLLECTIONS[collection_key]]

    return None
