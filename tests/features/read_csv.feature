          Feature: Read csv files into MongoDB

            Scenario: Read csv files from a collection directory
              When we run mongolyin.py with the correct arguments
              """
              tests/data/csv --address {address} --username {username} --password {password} --loglevel debug
              """
              Then it should upload the csv data into MongoDB
