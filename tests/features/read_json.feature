          Feature: Read json files into MongoDB

            Scenario: Read json files from a collection directory
              When we run mongolyin.py with the correct arguments
              """
              tests/data/json --address {address} --username {username} --password {password} --loglevel debug
              """
              Then it should upload the json data into MongoDB
