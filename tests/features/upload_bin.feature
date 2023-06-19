          Feature: Upload binary data to MongoDB

            Scenario: Upload new binary files from a collection directory
              When we run mongolyin.py with the correct arguments
              """
              tests/data/bin --address {address} --username {username} --password {password} --loglevel debug
              """
              Then it should upload the binary data into MongoDB
