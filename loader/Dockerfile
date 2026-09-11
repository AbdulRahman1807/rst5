FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .

RUN useradd --create-home --uid 1000 appuser
USER appuser

CMD ["python", "main.py"]
