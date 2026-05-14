FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY forwarder.py .

EXPOSE 8080

USER nobody
ENTRYPOINT ["python", "-u", "forwarder.py"]
