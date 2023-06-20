          Feature: Flexible file paths

            Scenario: File is inserted into database directory
              When we run mongolyin.py on a file with no collection directory
              """
              {inputdir} --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should upload the file into the default collection
              """
              misc
              """

            Scenario: File is inserted into root directory
              When we run mongolyin.py on a file with no database directory
              """
              {inputdir} --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
              """
              Then it should ignore the file
