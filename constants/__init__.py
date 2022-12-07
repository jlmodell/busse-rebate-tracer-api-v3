from .config import *
from .database_constants import *
from .environ import *
from .rebate_constants import *

from database import gc_rbt


def find_distinct_parts():
    collection = gc_rbt(SCHED_DATA)

    return list(collection.distinct("part"))


LIST_OF_DISTINCT_PARTS = find_distinct_parts()
