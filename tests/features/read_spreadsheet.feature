          Feature: Read spreadsheet files into MongoDB

            Scenario: Read csv files from a collection directory
              When we run mongolyin.py with the correct arguments
              """
              tests/data/csv --address {address} --username {username} --password {password} --loglevel debug
              """
              Then it should upload the spreadsheet data into MongoDB

            Scenario: Read ods files from a collection directory
              When we run mongolyin.py with the correct arguments
              """
              tests/data/ods --address {address} --username {username} --password {password} --loglevel debug
              """
              Then it should upload the spreadsheet data into MongoDB

            Scenario: Read xlsx files from a collection directory
              When we run mongolyin.py with the correct arguments
              """
              tests/data/xlsx --address {address} --username {username} --password {password} --loglevel debug
              """
              Then it should upload the spreadsheet data into MongoDB
