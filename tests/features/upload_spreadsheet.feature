          Feature: Upload spreadsheet data into MongoDB

            Scenario: Upload new csv files from a collection directory
              When we run mongolyin.py and copy files into the directory
              """
              tests/data/csv --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload the spreadsheet data into MongoDB

            Scenario: Upload new ods files from a collection directory
              When we run mongolyin.py and copy files into the directory
              """
              tests/data/ods --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload the spreadsheet data into MongoDB

            Scenario: Upload new xlsx files from a collection directory
              When we run mongolyin.py and copy files into the directory
              """
              tests/data/xlsx --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload the spreadsheet data into MongoDB

            Scenario: Upload new xls files from a collection directory
              When we run mongolyin.py and copy files into the directory
              """
              tests/data/xls --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload the spreadsheet data into MongoDB

            Scenario: Modify a csv file that already exists in the database
              Given we have existing csv data in the database
              When we run mongolyin.py and copy files into the directory
              """
              tests/data/csv --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload csv data for the modified file
