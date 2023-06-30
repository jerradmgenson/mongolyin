# mongolyin

mongolyin automatically ingests files in to MongoDB.

mongolyin makes the process of ingesting files almost effortless by allowing you to upload data simply by copying and pasting
files in your local filesystem. Directories map onto databases and collections, and new and modified files are automatically
detected by mongolyin and handled appropriately according to their file type and structure.

## Design Philosophy

1. mongolyin does one thing and does it well: automatically ingesting files into MongoDB.
  - It does not do file syncing.
  - It does not support any other DBMS than MongoDB.
  - It does not make pizza.
2. mongolyin aims to handle 90% of cases, not 100% of cases.
3. mongolyin is a background process, therefore it should be lightweight and not use all the resources on your system.
4. mongolyin prioritizes a low memory footprint over maximum upload speed.
5. mongolyin prioritizes correctness and robustness over performance.
6. mongolyin should be really easy to use.

## Setup Instructions

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

## Usage Instructions

Once you have mongolyin set up, you use it simply by copying files into the directory that it is monitoring. The only additional thing you need to do
is to create subdirectories for the databases and collections that you want mongolyin to upload your files to. Here is an example of what your directory
structure might look like:

```
/                         # root directory that mongolyin in watching
  /example_database1      # a directory corresponding to a MongoDB database
    /example_collection1  # a directory corresponding to a collection in example_database1
    /example_collection2  # another collection in example_database1
  /example_database2      # another database
    /example_collection3
```

mongolyin will automatically create new databases and collections if they don't already exist (and your MongoDB account has these privileges).

## File Types

mongolyin knows how to handle the following file types:
- **JSON**
  - When the JSON has an object (dict) as its root data structure, mongolyin will upload the entire file as one document.
  - When the JSON has an array (list) as its root data structure, mongolyin expects the child of the root data structure to be an object.
    It will upload objects in the array as one document per object. mongolyin will lazily extract and upload objects in a memory-efficient manner.
- **CSV**: mongolyin can handle most remotely-reasonable CSV structures automatically. It uses a combination of clevercsv and its own data type
  detection algorithms to automatically recognizes how the CSV is formatted and what the type of each column should be. mongolyin will upload
  each row in the CSV as one document per row, and will lazily extract and upload rows in a memory-efficient manner.
- **Excel**: mongolyin can handle both .xls and .xlsx files, as long as they are somewhat sanely formatted. Currently it only extracts the first sheet in
  the file. It uploads the file as one document per row.
- **ODS**: mongolyin handles ODS (ie LibreOffice/OpenOffice Calc) the same way that it handles Excel files.
- **Parquet**: mongolyin handles Parquet files the same way that it handles Excel and ODS files.
- **Other File Types**: all other file types than the ones mentioned here are treated as binary files and inserted into MongoDB using GridFS.