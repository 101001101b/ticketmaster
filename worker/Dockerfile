FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py .

EXPOSE 8000

# MODE 'direct' or 'indirect'
ENV MODE=direct

CMD ["python", "worker.py"]