FROM python:3.12-slim

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

CMD ["python3", "src/main.py"]
