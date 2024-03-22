Welcome to Odilon
-----------------

Odilon is a scalable and lightweight Open Source Object Storage that runs on standard hardware.
It is an infrastructure software to be used by applications that need to store large amounts 
of unstructured digital objects such as pdf, MS Word, XLSX files, backups, videos, photos or audio.
It has a simple single-level folder structure similar to the Bucket / Object model of Amazon S3. 
It is easy to integrate, offers encryption, data protection and fault tolerance (software RAID and Erasure Codes) 
and detection of silent data degradation.

https://odilon.io

How to Use
----------
To get started run

Windows
-------
cd .\bin
start.bat

Linux
-----
cd ./bin
.\start.sh


For a more in-depth introduction, please check out 
https://odilon.io


Support
-------
mail to info@novamens.com


Files included in an Odilon binary distribution
-----------------------------------------------

app/
  A self-contained Odilon server.
  
bin/
   Scripts to startup, manage and interact with Odilon instances. 

   Windows: 
   start.bat -> to start the server
   stop.bat -> to stop the server
   check.bat -> check if the server is running
   
   Linux: 
   start.sh -> to start the server
   stop.sh -> to stop the server
   check.sh -> to check if the server is running

config/
   odilon.properties -> Odilon Configuration file 
   log4j2.xml -> log configuration file

examples/
	Sample Java classes to create buckets, upload and download files, list objects, and others   
   
logs/
	application logs

