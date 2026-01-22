FROM python:3.11
LABEL authors="parham-barazesh"
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY ./ /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "hubspot_pipeline.py"]