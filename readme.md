#Reliable Log Streamer (QIAO)

Qiao (橋, the Chinese character for “bridge”, pronounced as Chiao) is a standalone service that streams event data from a log in real-time and delivers them to one or more destinations. Similar to Unix 'tail -F' command, Qiao keeps following a log file when it rotates. One notable difference of Qiao from other log streamers is that Qiao keeps track of read-cursor position and stores the offset on the disk efficiently. In the event that the agent restarts, Qiao is able to continue processing at the file position where it left off before it terminated even if the file was rotated out. Furthermore, for a space-conscious log producer, Qiao can process logs in a compacted binary format, in addition to regular text format (a single ‘line’ of text followed by line feed (‘\n’)). Qiao also supports Avro format out of box.

## Build status

![Build health](https://travis-ci.org/aol/qiao.svg)
![License](http://img.shields.io/badge/license-APACHE2-blue.svg)

###System Requirements
* Java 7
* Small amount of disk space for cursor position cache and history cache

##TO DO
* Resolve dependencies that are not accessible from github
