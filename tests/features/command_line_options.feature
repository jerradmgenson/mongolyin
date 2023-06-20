          Feature: Command line options

          Scenario: Server authentication given through environment variables
            Given we have defined our MongoDB username and password in environment variables
            When we run mongolyin.py without --username or --password
            """
            {inputdir} --address {address} --loglevel debug --sleep-time 0.001
            """
            Then it should correctly authenticate with the server

          Scenario: Server address given through environment variable
            Given we have defined our MongoDB server address in an environment variable
            When we run mongolyin.py without --address
            """
            {inputdir} --username {username} --password {password} --loglevel debug --sleep-time 0.001
            """
            Then it should correctly authenticate with the server

          Scenario: Auth DB given through environment variable
            Given we have defined our MongoDB auth db in an environment variable
            """
            admin
            """
            When we run mongolyin.py without --auth-db
            """
            {inputdir} --address {address} --username {username} --password {password} --loglevel debug --sleep-time 0.001
            """
            Then it should correctly authenticate with the server

          Scenario: Database name given on command line
            When we run monogolyin with --db
            """
            {inputdir} --address {address} --username {username} --password {password} --db {db} --loglevel debug --sleep-time 0.001
            """
            Then it should upload files to the correct location on the server

          Scenario: Collection name given on command line
            When we run monogolyin with --collection
            """
            {inputdir} --address {address} --username {username} --password {password} --collection {collection} --loglevel debug --sleep-time 0.001
            """
            Then it should upload files to the correct location on the server
