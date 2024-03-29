FROM tiangolo/uvicorn-gunicorn-fastapi:latest

WORKDIR /

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

EXPOSE 8188

COPY . /

RUN scrat init

CMD ["uvicorn", "--host", "0.0.0.0", "--port", "8188", "main:app"]