Woopra Coding Quiz
========

_Find the N most recent visitors_
 
This is a simplified MapReduce application, without using Hadoop or any other framework.
 
## Table of contents
 
- [Task Description](#task-description)
- [System Overview](#system-overview)
- [Usage](#usage)
 
## Task Description
 
Given a huge visitor sessions data, find out the 100 most recent visitors. The input sessions file contains line-separated JSON records. There might be multiple PIDs and/or activity records for one single user.
 
I finished the task in native Java, with a few restrictions and requirements:
 
 * The application is build with Maven;
 * A standalone class is made to parse and collect the raw JSON data;
 * The application includes a mapper and a reducer, and a collector, in the concept of MapReduce;
 * In theory, the application should run on multiple servers;
 * The final report includes visitor unique id ("pid") and their last seen timestamp ("time").
 
## System Overview
In general, the program includes five major components:
![System Overview](/doc/overview.png?raw=true "System Overview")
 
### Preprocess
 
It’s the optional stage to clean the input data. According to the JSON data, there are 2 types records: “visit” and “delete”. For “delete”, this visitor is assigned with a new pid. Therefore one single user could have multiple PIDs. The preprocess dedup these PIDs and updates all activity records with the latest PID:
 
1. Parser the file. When type is ”delete”, store the mapping from the old PID to the new one;
2. One visitor might change its PID for more than once. Therefore after reading all the records, process the PID mapping to link the old PIDs to the latest one;
3. Read the file again, replace the old PIDs with the new ones, using the generated PID mapping.
 
### Master
 
The master is a center server to coordinate all the following components. Its major job includes doing file sharding, bringing up mappers/reducer/collector, and dispatching the file shards to mappers.
 
### Data Sharding
 
The original file too large to be processed by any single mapper. It is sharded into smaller ones, whose size is suitable to be processed by one mapper;
 
### Mapper
 
The mapper processes one shard at one time. The mapper will ask for new shard from master to analyze until all file shards are processed, and notify the master when all jobs are done.
 
For our task, the mapper uses a modified heap (MapHeap) to keep track of the most recent visitors and outputs the PID/timestamps at the end of its life. The MapHeap is designed to support update operation in O(logN) time complexity, in addition to the standard heap operations:
 
1. Use array and map to implement this min-heap variant. The root of the MapHeap is the oldest visit record;
2. MapHeap’s size is less than or equal to the parameters RECENT_N_COUNT, as the maximum number of result records;
3. It provides the normal heap operations, such as put(), size(), pop() and getPeek();
4. When putting a new item and its PID with an older timestamp already exists in the heap, MapHeap would update the heap in the place. 
 
### Reducer
 
The reducer aggregates the temporary output from all mappers and identify the N most recent visitors. Usually there is a shuffle and sort stage at the beginning of reduce phase, or between map and reduce stages. But it does not make any difference in this application. Therefore I ignored that part.
 
For this specific task, one and only one reducer is needed. On one hand, the reducer needs to be aware of all visitor records to identify the N most recent visitors. On the other hand, one reducer is enough to handle all the data from mappers.
 
Each mapper produces 100 key-value records at most. Assuming that we have 10k mappers, which is an extremely large number for any MapReduce work, the reducer needs to handle 1 million records. Without any compaction or optimization, one record is about 27 bytes. Totally, even in this overwhelming case, the reducer only needs to process 27 megabyte, which is nearly too small to mention for a MapReduce application.
 
Like the mappers, the reducer also uses the MapHeap to maintain the N most recent visitors.
 
### Collector
 
Collector collects the output from reducer and writes the final report.
 
### Some Simplifications
 
As I said, this is a demo program to show the MapReduce concept and application. It actually needs at least two modifications to be a real distributed system:
 
 * I used local file system, which is accessible to all workers. Apparently a distributed file system, such as HDFS, is essential for a real MapReduce job;
 * I used multi-process to simulate the multiple servers. For a distributed system, we need a job controller system to launch processes on different servers.
 
In addition, it lacks of some must-have features for a distributed system, such as:
 
 * Heart beat: keep track of the status of mappers/reducers;
 * Failure recovery: if some mapper/reducer died, it needs to re-assign the job to another worker;
 * Single point of failure: if master died, we totally lost the track of the job; 
 * Progress tracking UI: show the progress of the whole job.
 
However, if we used Hadoop ecosystem to achieve our goal, the framework would do all these work for us.
 
## Usage
 
Please note that, the program is tested on the following platform configuration:
 * OS: macOS Sierra version 10.12.5 (16F73)
 * Java: javac 1.8.0_131
 * Maven: Apache Maven 3.5.0
 * Default locale: en_US
 * Platform encoding: UTF-8
 
The steps to run the program with the visitor sessions data is:
 
```bash
~/test $ /usr/local/bin/git clone https://github.com/wangsha365/WoopraTask
Cloning into 'WoopraTask'...
remote: Counting objects: 54, done.
remote: Compressing objects: 100% (24/24), done.
remote: Total 54 (delta 13), reused 50 (delta 13), pack-reused 0
Unpacking objects: 100% (54/54), done.
 
~/test $ cd WoopraTask/
~/test/WoopraTask $ mvn test  # optional
……………….
-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running com.example.task.recent100.MapHeapTest
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.011 sec
Running com.example.task.recent100.UserRecordTest
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0 sec
 
Results :
 
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
…………….
 
# If you ran `mvn clean compile`, you need to download the dependencies and include their locations into the CLASSPATH.
~/test/WoopraTask $ mvn package
~/test/WoopraTask $ export CLASSPATH="${CLASSPATH}:.:`pwd`/target/recent100-0.1-jar-with-dependencies.jar"
 
~/test/WoopraTask $ java com.example.task.recent100.DataPreprocess $HOME/Documents/test/file.txt
Preprocess done. New file: /Users/shawang/Documents/test/file.txt_dedup
 
~/test/WoopraTask $ java com.example.task.recent100.Master -h
usage: Master
 -f <filename>      The file is a line separated JSON, containing records
                    of visitor sessions.
 -h                 help information
 -m <mapperCount>   The count of Mapper.
 -r <resultCount>   The count of records in report.
 -s <size>          The number of records per shard.
 
~/test/WoopraTask $ java com.example.task.recent100.Master -f $HOME/Documents/test/file.txt_dedup
MapReduce arguments:
  Input: /Users/shawang/Documents/test/file.txt_dedup
  #(Most Recent Visit) : 100
  #(Records) Per Shard: 200
  #(Mapper): 4
  #(Reducer): 1 (non-configurable)
Started server: 192.168.1.168:61578
Started 4 Mapper.
Mappers finished!
Started 1 Reducer.
Reducer finished!
All DONE!
Report is: /Users/shawang/Documents/test/file.txt_dedup_report.csv
```
 
The final report is in CSV format. In each line, the first column is the ranking as the latest visitor. The second column is the visitor PID. And the last column is the timestamp of the visitor’s last access. A snippet is as following:
 
> 1,B0RWNatCQJBg,1491116371836
> 
> 2,k6vApa6SMYud,1491116291575
> 
> 3,1zRqzEdY4hda,1491116180719
> 
> 4,QSrCfNxkEiZu,1491116153015
> 
> 5,1PtCKFDkhmfB,1491116149699
