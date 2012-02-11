Wuchubuzai Random User Generator
================================

Code that creates random user data in [Apache Cassandra](http://cassandra.apache.org/).  Enables the capability to model random data and work with the random data in a Cassandra cluster
to enable tests to be run for various different exercises.      

Default generation is for *100,000* users `(1000 users per thread * 100 threads)`.  These settings can be changed in the code, or via the properties file

Tested With
-----------
    
* Apache Cassandra 1.0.7 http://cassandra.apache.org/ `Windows` 
* java version "1.7.0", Java(TM) SE Runtime Environment (build 1.7.0-b147), Java HotSpot(TM) 64-Bit Server VM (build 21.0-b17, mixed mode)
    

Properties File
---------------

    ### property file to override application defaults
    # set singleTest to true to enable the generation of one user that is not saved to Cassandra 
    # singleTest=true
    
    ### to specificy alternate information about the Cassandra node
    # keyspace = random 
    # userRowKey = users
    # userCf = userCf
    # hostIp = 127.0.0.1:9160
    # clusterName = testCluster
    
    ### the following properties are the defaults, which will create 100,000 users
    # numUsers = 1000
    # numThreads = 100


Contributing
------------

1. Fork it.
2. Commit your changes
3. Push it
4. Create an [Issue][1] with a link to your branch


[1]: https://github.com/wuchubuzai/randomUsers/issues