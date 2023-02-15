from typing import List, Tuple


def SET_COLUMNS(
    hidden: str,
    gpo: str,
    lic: str,
    cs_conv: str,
    score: str,
    check: str,
    contract: str,
    claim_nbr: str,
    order_nbr: str,
    invoice_nbr: str,
    invoice_date: str,
    part: str,
    ship_qty: str,
    unit_rebate: str,
    rebate: str,
    name: str,
    addr: str,
    city: str,
    state: str,
    postal: str,
    uom: str,
    cost: str,
    added_to_queue: str,
) -> Tuple[List[str], List[str]]:

    original_columns = [
        hidden,
        name,
        addr,
        city,
        state,
        postal,
        gpo,
        lic,
        score,
        check,
        contract,
        claim_nbr,
        order_nbr,
        invoice_nbr,
        invoice_date,
        part,
        unit_rebate,
        ship_qty,
        uom,
        cs_conv,
        rebate,
        cost,
        added_to_queue,
    ]

    output_columns = [
        "period",
        "name",
        "addr",
        "city",
        "state",
        "postal",
        "gpo",
        "license",
        "searchScore",
        "check license",
        "contract",
        "claim_nbr",
        "order_nbr",
        "invoice_nbr",
        "invoice_date",
        "part",
        "unit_rebate",
        "ship_qty",
        "uom",
        "ship_qty_as_cs",
        "rebate",
        "cost",
        "added_to_queue",
    ]

    print("original_columns", original_columns)
    print("output_columns", output_columns)

    return (
        original_columns,
        output_columns,
    )
