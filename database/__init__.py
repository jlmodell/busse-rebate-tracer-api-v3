from .connector import *
from constants import CONTRACTS, PRICING_CONTRACTS


def gc_rbt(collection_key: str) -> Collection:
    client = GET_CLIENT()
    db = GET_DATABASE(client, BUSSE_REBATE_TRACES)

    return GET_COLLECTION(db, collection_key)


def gc_bp(collection_key: str) -> Collection:
    client = GET_CLIENT()
    db = GET_DATABASE(client, BUSSE_PRICING)

    return GET_COLLECTION(db, collection_key)


def delete_documents(collection: Collection, filter: dict) -> None:
    res = collection.delete_many(filter)
    print(res)


def insert_documents(collection: Collection, documents: list) -> None:
    res = collection.insert_many(documents)
    print(res)


def get_current_contracts():
    collection = gc_rbt(CONTRACTS)

    return {x["contract"]: x["gpo"]
            for x in list(collection.find({"valid": True}))}


def find_contract_by_contract_number(contract_number: str = None) -> dict:
    assert contract_number is not None, "contract_number cannot be None"

    collection = gc_bp(PRICING_CONTRACTS)

    res = collection.find_one({
        "contractnumber": contract_number,
    }, {"_id": 0})

    return res


def get_documents(collection: Collection, filter: dict) -> list:
    return list(collection.find(filter))
