          Feature: Upload json data to MongoDB

            Scenario: Upload new json files from a collection directory
              When we run mongolyin.py and copy files into the directory
              """
              tests/data/json --address {address} --username {username} --password {password} --loglevel debug
              """
              Then it should upload the json data into MongoDB
