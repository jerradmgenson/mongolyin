          Feature: Upload json data to MongoDB

            Scenario: Upload new json files from a collection directory
              When we run mongolyin.py and copy files into the directory
              """
              tests/data/json --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload the json data into MongoDB

            Scenario: Modify a json file that already exists in the database
              Given we have existing json data in the database
              When we run mongolyin.py and copy files into the directory
              """
              tests/data/json --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload json data for the modified file

            Scenario: Upload a JSON file greater than 16793598 bytes
              When we run mongolyin.py and copy a large json file into the directory
              """
              {inputdir} --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload the json data using GridFS
