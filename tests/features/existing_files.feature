          Feature: Monitor a directory with preexisting files

            Scenario: Monitor a directory with preexisting files
              Given we have a directory with preexisting files
              """
              tests/data/json
              """
              When we run mongolyin.py on the directory with preexisting files
              """
              tests/data/json --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should not upload the files
