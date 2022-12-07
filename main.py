from fastapi import FastAPI, BackgroundTasks, HTTPException, Form, Request, Response
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from constants.database_constants import TRACINGS
from database import delete_documents, gc_rbt, insert_documents
from finders import find_tracings_and_save
from s3_functions import get_field_file_body_and_decode_kwargs
from s3_functions.getters import get_list_of_files
from transformers import (
    build_df_from_warehouse_using_fields_file,
    ingest_to_data_warehouse,
)

import re

MONTHS = {
    "01": "January",
    "02": "February",
    "03": "March",
    "04": "April",
    "05": "May",
    "06": "June",
    "07": "July",
    "08": "August",
    "09": "September",
    "10": "October",
    "11": "November",
    "12": "December",
}

app = FastAPI()

templates = Jinja2Templates(directory="templates")

origins = [
    "http://localhost",
    "http://localhost:8000",
    "https://rebate_tracing_tool.bhd-ny.com/",
    "https://http://128.1.5.76:8188/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def insert_tracings(field_file_name: str):
    field_file = get_field_file_body_and_decode_kwargs("input/", field_file_name)
    df = build_df_from_warehouse_using_fields_file(field_file_name)
    collection = gc_rbt(TRACINGS)

    try:
        delete_documents(collection, {"period": field_file.get("period")})
        insert_documents(collection, df.to_dict("records"))
    except Exception as e:
        print(e)


@app.get("/")
def read_root():
    return {"process": "/ingest_file", "update": "/update_tracings"}


@app.get("/ingest_file", response_class=HTMLResponse)
async def ingest_file_form(request: Request):
    title = "Ingest File to Data Warehouse"

    regex_field_file = re.compile(
        r"(backups|gpo_report|^input/$|old|updated|13104)", re.IGNORECASE
    )

    prefix = "input/"

    field_files = get_list_of_files(prefix)

    field_files = [
        field_file.replace(prefix, "")
        for field_file in field_files
        if regex_field_file.search(field_file) is None
    ]

    context = {
        "request": request,
        "title": title,
        "field_files": field_files,
    }

    return templates.TemplateResponse("ingest_file.html", context)


@app.get("/files")
async def get_files(request: Request, month: str = "", year: str = ""):
    key = f"rebate_trace_files/{MONTHS[month].lower()} {year}/"

    print(key)

    regex_file = re.compile(r"complete", re.IGNORECASE)

    field_files = get_list_of_files(key)

    field_files = [
        field_file.replace(key, "")
        for field_file in field_files
        if regex_file.search(field_file) is None
    ]

    context = {
        "request": request,
        "field_files": field_files,
    }

    return templates.TemplateResponse("fragments/files.html", context)


@app.post("/ingest_file")
async def ingest_file(
    month: str = Form(...),
    year: str = Form(...),
    file_name: str = Form(...),
    field_file_name: str = Form(...),
    delimiter: str = Form(default=","),
    header_row: str = Form(default="0"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    prefix = "rebate_trace_files/"

    if month not in MONTHS:
        raise HTTPException(status_code=400, detail="Invalid month")

    storage_key = f"{MONTHS[month].lower()} {year}/{file_name}"

    background_tasks.add_task(
        ingest_to_data_warehouse,
        file_path=None,
        prefix=prefix,
        key=storage_key,
        year=year,
        month=month,
        overwrite=True,
        delimiter=delimiter,
        header_row=int(header_row),
    )

    background_tasks.add_task(insert_tracings, field_file_name)

    background_tasks.add_task(
        find_tracings_and_save,
        month=MONTHS[month],
        year=year,
        overwrite=True,
    )

    return {
        "debug": {
            "prefix": prefix,
            "month": month,
            "month_string": MONTHS[month],
            "year": year,
            "field_file_name": field_file_name,
            "file_name": file_name,
            "delimiter": delimiter,
            "header_row": header_row,
        },
        "output": {"key": storage_key, "field_file": field_file_name},
        "status": "success",
    }


@app.get("/update_tracings", response_class=HTMLResponse)
async def get_update_tracings(request: Request):
    title = "Update tracings from Data Warehouse"
    context = {
        "request": request,
        "title": title,
    }

    return templates.TemplateResponse("update_tracings.html", context)


@app.post("/update_tracings")
async def post_update_tracings(
    request: Request, month: str = Form(...), year: str = Form(...)
):

    df = find_tracings_and_save(MONTHS[month], year, overwrite=True)

    return HTMLResponse(df.to_html(index=False))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
