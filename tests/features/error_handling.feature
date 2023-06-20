          Feature: Graceful error handling

            Scenario: Corrupt json file can not be parsed
              Given that we have a directory structure with a corrupt json file
              When we run mongolyin.py on that directory
              """
              {inputdir} --address {address} --username {username} --password {password} --loglevel debug
              """
              Then it should log the error and continue without crashing
