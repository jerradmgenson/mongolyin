          Feature: Graceful error handling

            Scenario: Corrupt json file can not be parsed
              Given that we have a directory structure with a corrupt json file
              When we run mongolyin.py on that directory
              """
              {inputdir} --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should log the error and continue without crashing
              """
              json.decoder.JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)
              """

            Scenario: Server is down during upload
              Given that the MongoDB server is down
              When we run mongolyin.py while the server is down and copy files into the directory
              """
              tests/data/json --address {address} --username {username} --password {password} --loglevel debug
              """
              Then it should log the error
              And upload the files when the server is back up
