FROM python:3.10-slim
WORKDIR /app
ADD ./mongolyin /app/mongolyin
ADD ./requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

# Run mongolyin when the container launches
CMD ["python", "-m", "mongolyin.mongolyin", "/media/ingest_dir"]
