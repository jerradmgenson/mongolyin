FROM python:3.10-slim
WORKDIR /app
ADD ./run_mongolyin.sh /app
ADD ./mongolyin /app/mongolyin
ADD ./requirements.txt /app
RUN chmod +x /app/run_mongolyin.sh
RUN pip install --no-cache-dir -r requirements.txt

# Run mongolyin when the container launches
CMD ["/app/run_mongolyin.sh", "/media/ingest_dir"]
