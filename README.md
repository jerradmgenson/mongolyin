# mongolyin

## Installation Instructions

1. Install Docker engine on your computer.
2. Download mongolyin source code from GitHub.
3. Open a terminal and cd into the directory where you downloaded mongolyin.
4. Create a file called ".env" with the following environment variables defined with your MongoDB username, password, and server address (respectively):
  * MONGODB_USERNAME
  * MONGODB_PASSWORD
  * MONGODB_ADDRESS
5. Build the Docker image with:
```
docker build -t mongolyin .
````
6. Run the image with:
```
docker run -d --env-file .env --name mongolyin --restart always -v {path_to_watch_dir}:/media/ingest_dir mongolyin
```
Be sure to replace `{path_to_watch_dir}` with the actual directory you want to monitor.

7. Enjoy!
