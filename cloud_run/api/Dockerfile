FROM tiangolo/uvicorn-gunicorn-fastapi:latest
RUN mkdir -p /code
RUN mkdir -p /.gcloud
WORKDIR /code
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY .gcloud/ /.gcloud/
COPY . /code/
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
