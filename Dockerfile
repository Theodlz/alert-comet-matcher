FROM python:3.10

COPY main.py /app/main.py
COPY requirements.txt /app/requirements.txt
COPY .env /app/.env

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 8265

CMD ["python", "main.py"]
