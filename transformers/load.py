import re
from functools import lru_cache

from constants import ATLAS_SEARCH_INDEX_NAME, BOX_ALIAS, CASE_ALIAS, EACH_ALIAS


@lru_cache(maxsize=None)
def FIX_NAME(name: str) -> str:
    name = (
        "martin health system"
        if re.search(r"tradition medical center", name, re.IGNORECASE)
        else name
    )
    name = (
        "cleveland clinic rehabilitation hospital - edwin shaw"
        if re.search("edwin sha", name, re.IGNORECASE)
        else name
    )
    name = "roper hospital" if re.search(r"rh stock", name, re.IGNORECASE) else name
    name = (
        "texas health harris southwest"
        if re.search(r"^thc", name, re.IGNORECASE)
        else name
    )
    name = "kintegra health" if re.search(r"^kh", name, re.IGNORECASE) else name
    name = (
        "ur orthopaedics (victor)"
        if re.search("university of roch", name, re.IGNORECASE)
        else name
    )
    name = (
        "chambersburg hospital warehouse"
        if re.search(r"^chws", name, re.IGNORECASE)
        else name
    )
    name = "chavies clinic" if re.search(r"^shifa", name, re.IGNORECASE) else name
    name = (
        "mercy physician enterprises"
        if re.search(r"lambert family", name, re.IGNORECASE)
        else name
    )
    name = (
        "rochester general hospital"
        if re.search(r"^rgh", name, re.IGNORECASE)
        else name
    )
    name = (
        "west virginia university hospitals"
        if re.search(r"ruby memorial", name, re.IGNORECASE)
        else name
    )
    name = (
        "geisinger wyoming valley medical center"
        if re.search(r"^gwv", name, re.IGNORECASE)
        else name
    )
    name = (
        "northwell health" if re.search("^brett ruffo", name, re.IGNORECASE) else name
    )
    name = (
        "the queens medical center" if re.search(r"qmcp", name, re.IGNORECASE) else name
    )
    name = "lincolnhealth" if re.search("lincoln health", name, re.IGNORECASE) else name
    name = (
        "albany ent and allergy svss pc"
        if re.search(r"^kinetic sports", name, re.IGNORECASE)
        else name
    )
    name = (
        "ucimc distribution center"
        if re.search("uc irvine med", name, re.IGNORECASE)
        else name
    )
    name = (
        "central oklahoma family medicine center"
        if re.search(r"cofmc", name, re.IGNORECASE)
        else name
    )
    name = (
        "university health associates"
        if re.search("waynesburg outpatient clinic", name, re.IGNORECASE)
        else name
    )
    name = (
        "respiratory services of wny"
        if re.search(r"^rswny", name, re.IGNORECASE)
        else name
    )
    name = "connect life" if re.search(r"^unyts", name, re.IGNORECASE) else name
    name = "journeycare" if re.search(r"^jc", name, re.IGNORECASE) else name
    name = (
        "the neurology center"
        if re.search(r"^the neurology center", name, re.IGNORECASE)
        else name
    )
    name = (
        "marathon health"
        if re.search(r"^marathon health", name, re.IGNORECASE)
        else name
    )
    name = (
        "dignity gohealth urgent care, llc"
        if re.search(r"(^hartford|^henry|).*go(-|)health", name, re.IGNORECASE)
        else name
    )
    name = (
        "baylor dermatology"
        if re.search(r"^baylor college", name, re.IGNORECASE)
        else name
    )
    name = (
        "exceptional emergency center"
        if re.search(r"^exceptional health", name, re.IGNORECASE)
        else name
    )
    name = (
        "vitality medical" if re.search(r"^vitality med", name, re.IGNORECASE) else name
    )
    name = (
        "oak street health - truman"
        if re.search(r"^oak street health", name, re.IGNORECASE)
        else name
    )

    name = (
        "carbon health - direct urgent care"
        if re.search(r"^direct urgent care", name, re.IGNORECASE)
        else name
    )
    name = (
        "everside health"
        if re.search(r"^everside health", name, re.IGNORECASE)
        else name
    )
    name = "prime md" if re.search(r"primemd$", name, re.IGNORECASE) else name
    name = (
        "pediatrics in brevard"
        if re.search(r"^pediatric in brevard", name, re.IGNORECASE)
        else name
    )
    name = (
        "beaumont emergency center"
        if re.search(r"^beaumont elite emergency", name, re.IGNORECASE)
        else name
    )
    name = (
        "central ohio primary care"
        if re.search(r"^central ohio primary", name, re.IGNORECASE)
        else name
    )
    name = (
        "umansky medical center for plastic surgery"
        if re.search(r"^umansky surgery", name, re.IGNORECASE)
        else name
    )
    name = (
        "advanced lifeline respiratory services"
        if re.search(r"^als regency", name, re.IGNORECASE)
        else name
    )
    name = (
        "georgia endoscopy center, llc"
        if re.search(r"^georgia endoscopy center", name, re.IGNORECASE)
        else name
    )
    name = (
        "global medical response" if re.search(r"^gmr", name, re.IGNORECASE) else name
    )
    name = (
        "orthoarizona scottsdale asc"
        if re.search(r"^orthoarizona scott", name, re.IGNORECASE)
        else name
    )
    name = (
        "sinus surgery center - idaho pa"
        if re.search(r"^sinus surgery center.*idaho pa$", name, re.IGNORECASE)
        else name
    )
    name = (
        "united health centers of the san joaquin valley"
        if re.search(r"^united family health", name, re.IGNORECASE)
        else name
    )
    name = (
        "valley medical pharmacy inc"
        if re.search(r"^manor drugs", name, re.IGNORECASE)
        else name
    )
    name = (
        "phrs pharmacy"
        if re.search(r"^pediatric home service", name, re.IGNORECASE)
        else name
    )
    name = (
        "sono bello - corporate"
        if re.search(r"^sono bello", name, re.IGNORECASE)
        else name
    )
    name = (
        "cerebral palsy association of the north country"
        if re.search(r"community health ctr of north country", name, re.IGNORECASE)
        else name
    )
    name = (
        "centurion operating at reception and medical center - main and hospita..."
        if re.search(r"^rmc", name, re.IGNORECASE)
        else name
    )
    name = (
        "alexian brothers child care center"
        if re.search(r"^amita north", name, re.IGNORECASE)
        else name
    )
    name = (
        "s m digestive diagnostic"
        if re.search(r"^sm digest", name, re.IGNORECASE)
        else name
    )
    name = (
        "tulsa pediatric urgent care"
        if re.search(r"^tulsa pediatric urgent", name, re.IGNORECASE)
        else name
    )
    name = (
        "holden rehab and skilled nursing"
        if re.search(r"^holden nursing", name, re.IGNORECASE)
        else name
    )
    name = (
        "g.a. carmichael family"
        if re.search(r"^ga carmichael", name, re.IGNORECASE)
        else name
    )

    return name


def FIX_ADDRESS(addr1: str, addr2: str) -> str:
    if addr2 == "":
        return addr1

    if re.search(r"\d+", addr1, re.IGNORECASE) and not re.search(
        r"((^C\/O)|(ATT(N|)|ACC(OUNT|T|)|PAY(AB|ABL|ABLE|)|((NO|NO |)DE(L |L|LI|LIV|LIVER|LIVERY)))|(A(C\d|C\d\d)))|(^S(UITE|TE ))|(^\#)|(ORDER( #|#))|(DEPT( |))",
        addr1,
        re.IGNORECASE,
    ):
        return addr1

    return addr2


def BUILD_SHOULD_QUERY(
    name: str = None, address: str = None, city: str = None, state: str = None
):
    should = []

    if name:
        should.append(
            {
                "text": {
                    "query": name.lower().strip(),
                    "path": "alias",
                    "score": {
                        "boost": {"value": 8},
                    },
                }
            }
        )

    if address:
        should.append(
            {
                "text": {
                    "query": address.lower().strip(),
                    "path": "address",
                    "score": {
                        "boost": {"value": 5},
                    },
                },
            }
        )

    if city:
        should.append(
            {
                "text": {
                    "query": city.lower().strip(),
                    "path": "city",
                    "score": {
                        "boost": {"value": 4},
                    },
                },
            }
        )

    if state:
        should.append(
            {
                "text": {
                    "query": state.lower().strip(),
                    "path": "state",
                    "score": {
                        "boost": {"value": 2},
                    },
                },
            }
        )

    return should


def BUILD_AGGREGATION(
    group: str = "",
    name: str = None,
    address: str = None,
    city: str = None,
    state: str = None,
):
    should = BUILD_SHOULD_QUERY(name=name, address=address, city=city, state=state)

    aggregation = [
        {
            "$search": {
                "index": ATLAS_SEARCH_INDEX_NAME,
                "compound": {
                    "should": should,
                },
            },
        },
        {
            "$match": {
                "group_name": group.upper(),
            },
        },
        {
            "$limit": 1,
        },
        {
            "$project": {
                "_id": 1,
                "member_id": 1,
                "alias": 1,
                "name": 1,
                "address": 1,
                "city": 1,
                "score": {"$meta": "searchScore"},
            }
        },
    ]

    return aggregation


def CONVERT_UOM(item: dict, uom: str, qty: int) -> float:
    if item["part"] in ["139", "153", "283", "284", "7190"] and uom in BOX_ALIAS:
        return qty

    if item["part"] in ["164"] and re.search(r"^pk", uom, re.IGNORECASE):
        boxes = item["num_of_dispenser_boxes_per_case"]

        return qty / boxes if boxes != 0 else 0

    if item["part"] in ["770"] and re.search(r"^pk", uom, re.IGNORECASE):
        qty = qty * 12
        eaches = 48

        return qty / eaches

    if item["part"] in ["3220"] and re.search(r"^ct", uom, re.IGNORECASE):
        return (
            qty / item["num_of_dispenser_boxes_per_case"]
            if item["num_of_dispenser_boxes_per_case"] != 0
            else 0
        )

    if item in ["795"] and re.search(r"^pk", uom, re.IGNORECASE):
        eaches = item["each_per_case"]

        return qty / eaches if eaches != 0 else 0

    try:
        eaches = item["each_per_case"]
        boxes = item["num_of_dispenser_boxes_per_case"]

        if uom in EACH_ALIAS:
            return qty / eaches if eaches != 0 else 0
        elif uom in BOX_ALIAS:
            return qty / boxes if boxes != 0 else qty / eaches if eaches != 0 else 0
        elif uom in CASE_ALIAS:
            return qty
        else:
            return 0

    except TypeError:
        print("******************************************")
        print(TypeError)
        print(item, uom, qty)
        print("******************************************")

        return 0
