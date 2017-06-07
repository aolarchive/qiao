#Reliable Log Streamer (QIAO)

Qiao (橋, the Chinese character for “bridge”, pronounced as Chiao) is a standalone service that streams event data from a log in real-time.  Qiao delivers event data to one or more destinations. Similar to Unix 'tail -F' command, Qiao can continue following a log file even after it rotates. One notable difference of Qiao from other log streamers is that Qiao keeps track of read-cursor position and stores the offset on the disk efficiently. In the event that the agent restarts, Qiao is capable of resuming processing at the file position where it left off before it terminated even if the file was rotated out. Qiao supports logs written in Avro format.  Avro format is the most common format used by Qiao users.  Furthermore, for some space-conscious log producers, Qiao can process logs in a compacted binary format.  Qiao also support log files in regular text format (a single ‘line’ of text followed by line feed (‘\n’)). 

## Build status

![Build health](https://travis-ci.org/aol/qiao.svg)
![License](http://img.shields.io/badge/license-APACHE2-blue.svg)

## System Requirements
* Java 8
* Small amount of disk space for cursor position cache and history cache


## Release Notes
### Version 0.4-SNAPSHOT (June 2017)
* Multi-agent support which enables streaming multiple log files in parallel
* Schema change in qiao.xml.  Qiao.xml in the old schema is required to be modified by adding an <agent> tag around <funnel>.
* Java 8 


### Version 0.3-SNAPSHOT (Feb 2017)
* Quarantine corrupted avro logs

