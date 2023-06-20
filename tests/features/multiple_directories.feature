          Feature: Multiple directories

            Scenario: Files inserted into multiple collection directories
              When we run mongolyin.py on a path with multiple collection directories
              """
              {inputdir} --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload files from both directories
