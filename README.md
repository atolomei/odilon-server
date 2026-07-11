![spring-gaede65182_1280](https://github.com/atolomei/odilon-server/assets/29349757/f1c6f491-9d1f-4e4d-af87-f7e57713542a)

<p><b>NEWS</b>: Odilon Server 2.1 available (18 May 2026). Odilon client SDK 2.0 available (18 May 2026)</p>

<h1>Odilon Object Storage</h2>


<h2>Enterprise-class storage made simple</h2>
<p>Odilon is a simple, secure, fully Open Source object storage server that runs on standard hardware.
	(<a href="https://odilon.io" target="_blank">Odilon project website</a>).
</p>

<p>Designed for small and medium-scale deployments (up to roughly 100 TB), it provides reliable storage for documents, images, audio, video, and other large files without the complexity of distributed systems.
</p>

<p>It includes encryption, software RAID, erasure coding, versioning, and Internet-based replication under a clear standard license with no hidden restrictions.
</p>

<p>Ideal for organizations that need secure, reliable object storage without depending on cloud providers or large-scale distributed infrastructure.
</p>


<h2>Main features</h2>
				<p>
				<ul>
				<li> Scalable Object Storage on commodity disks</li>
				<li>Single binary, does not need a database or other external software</li>
    				<li>It has a simple single-level folder structure similar to the Bucket/Object model of <a href="https://aws.amazon.com/s3 /" target="_blank">Amazon S3</a></li>					
				<li> Runs on Linux, Mac and Windows</li>				
				<li> SDK Java 11+ for client applications</li >
				<li> HTTP/S for client server communication</li>
				<li>License <a href="https://www.apache.org/licenses/LICENSE-2.0" target="_blank">Open Source Apache 2</a>. It can be used for Open Source and commercial projects </li>
				<li>Encryption <i>at rest</i> (<a href="https://es.wikipedia.org/wiki/Advanced_Encryption_Standard" target="_blank">AES GCM-SIV</a>) </li>
				<li>Simple operation. Adding new disks requires one line in the config file, and an <i>async process</i> sets up disks and replicata data in background</li>
				<li>Data replication using <a href="https://en.wikipedia.org/wiki/Erasure_code" target="_blank">Erasure Coding</a> and <a href="https://en.wikipedia.org/wiki/RAID" target="_blank">software RAID</a>. Tolerates full disk failures</li>
				<li>Data immutability. Odilon supports two storage modes that protect data from deletion, whether accidental or intentional: Read Only and <a href="https://en.wikipedia.org/wiki/Write_once_read_many" target="_blank">WORM</a> (Write Once Read Many)
				<li>Master - Standby architecture with async replication over the web, for disaster recovery, high availability, archival, ransomware recovery</li>
				<li>Version Control</b>. Odilon supports two modes: standard and protected version control, designed for compliance and ransomware protection, where previous versions can not be deleted</li>
				<li>Integration with Key Management Server <a href="https://www.vaultproject.io/" target="_blank">Hashicorp Vault</a> </li>
				<li>Disk monitoring for silent and slow data degradation detection (<a href="https://en.wikipedia.org/wiki/Data_degradation" target="_blank" >bit rot detection</a>)</li>
				<li> Developed in Java, the server requires Java 17+ (uses <a href="https://spring.io/projects/spring-boot">Spring Boot</a>, <a href="https://square.github.io/okhttp/">OkHttp</a>, <a href="https://github.com/FasterXML/jackson">Jackson</a>, <a href="https://github.com/ben-manes/caffeine">Caffeine</a>, <a href="https://metrics.dropwizard.io/4.2.0/">Metrics</a>, among others) </li>
				</ul>
				</p>

<h2>Security</h2>
<p>Odilon keeps objects encrypted (<i>Encryption at Rest</i>) using <a href="https://es.wikipedia.org/wiki/Advanced_Encryption_Standard" target="_blank">AES GCM-SIV</a> wwith keys size of 256 bits (v2.1+) or 128 bits (v2.0 or older). 
	
</p>
	<p>Encryption has the following benefits: <br/>	
<ul>
 <li>It simplifies data protection, applications dont need to worry about protecting the data or encryption keys.</li>
 <li>If data falls into the hands of an attacker, they cannot read it without also having access to the encryption keys. If attackers obtain the storage devices containing the data, they will not be able to understand or decrypt it.</li>
<li> It helps reduce the attack surface by removing lower layers of the hardware and software stack.</li>
 <li>Simplify security management, centrally managed encryption keys create a single place where data access is enforced and can be audited.</li>
 <li>It provides a privacy mechanism by limiting the access that systems and engineers have to data.</li>
</ul>
</p>
 
<p>Odilon uses <i>envelope encryption</i> (i.e. encrypting a key with another key), every object is encrypted with its unique key and the key is encrypted by Odilon key management layer or by a Key Management Server (<a href="https://en.wikipedia.org/wiki/Key_management" target="_blank ">KMS</a>)</p>

<p>A KMS is software for generating, distributing, and managing cryptographic keys. It includes back-end functionality for key generation, distribution, and replacement. Moving key management to KMS prevents application reverse engineering attacks, simplifies operational maintenance, and compliance with security policies and regulations.</p>
<p>Odilon integrates with the KMS Open Source <a href="https://www.vaultproject.io/" target="_blank">Hashicorp Vault</a>.</p>
 
<h2>Data Replication</h2>
Odilon can be configured to use software RAID for data replication. The supported configurations are
 
<ul>
<li><b>RAID 0.</b> Two or more disks are combined to form a volume, which appears as a single virtual drive.
It is not a configuration with data replication, its function is to provide greater storage and performance by allowing access to the disks in parallel.<br/><br/>
</li>
<li><b>RAID 1.</b>For each object, 1 or more exact copies (or mirrors) are created on two or more disks. This provides redundancy in case of disk failure. At least 2 disks are required, Odilon also supports 3 or more for greater redundancy.<br/><br/>
</li>
<li><b>Erasure Coding.</b>
	It is a method of encoding data into blocks that can be distributed across multiple disks or nodes and then reconstructed from a subset of those blocks. It has great flexibility since you can adjust the number and size of the blocks and the minimum required for recovery. It uses less disk space than RAID 1 and can withstand multiple full disk failures.<br/> <br/>  
	
	Odilon implements this architecture using <a href="https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction" target="_blank"> Reed Solomon</a> error-correction codes.<br/> <br/> 
	Reed Solomon codes are widely used, from the Voyager probes exploring the solar system, to the James Webb telescope, to the QR codes on mobile phones.<br/> <br/> 
	Odilon's Erasure Coding architecture is designed to recover data automatically after one or more disk failures.<br/> <br/>  	
	It can also detect and repair silent data corruption caused by bad sectors, bit flips, or other storage errors. It has a background data scrubber that periodically walks every object in storage and verifies its integrity, and also an optional read-repair functionality that checks the integrity of each shard on every read operation.<br/>  <br/>  
The configurations are: <br/> <br/> 
<b>3 disks</b> (2 data 1 parity, supports 1 full disk failure), <br/>  
<b>6 disks</b> (4 data 2 parity, supports up to 2 full disks failures) <br/>
<b>12 disks</b> (8 data 4 parity, supports up to 4 full disks failures) <br/>
<b>24 disks</b> (16 data 8 parity, supports up to 8 full disks failures)<br/>
<b>48 disks</b> (32 data 16 parity, supports up to 16 full disks failures)<br/>
</li> <br/>

</ul>






<h2>Version Control</h2>

<p>Odilon supports two version control modes: standard and protected (see manual page how to configure version control).</p>

<p>The standard mode is the usual version control mode, where updating an object generates a new version, and client applications have the control to delete previous versions.</p>

<p>The protected mode is designed for compliance or ransomware protection; client applications can create new versions of an object but cannot delete previous versions, nor can they delete stored objects.</p>

<p>These three version control configurations (disabled, standard, protected) are complementary to the three Data Storage modes: rw(normal read-write), Read Only for immutable servers, and WORM (Write Once Read Many) where objects can be created and accessed but cannot be updated or deleted..</p>




<h2>Master Standby Architecture</h2>
<p>Odilon supports Master - Standby Architecture for <b>disaster recovery</b>, <b>high availability</b>, <b>archival</b>, and <b>anti-ransomware</b> protection. Data replication is done asynchronously using HTTP/S over the local network or the Internet. Setting up a standby server is simple, just add the URL and credentials to the master configuration file and restart. 
Odilon will propagate each operation to the standby server. It will also run a replication process in background for data existing before connecting the standby server. 
<br/>
<br/>
<br/>
<br/>
​</p>


![odilon-master-standby](https://github.com/atolomei/odilon-server/assets/29349757/913f7b54-1acf-46a2-97c6-3bd42190b9af)


<br/>
<br/>
<br/>
<br/>
<h2>What Odilon is not</h2>
<p>
<ul class="group-list>
<li class="list-item"><b>Odilon is not a Distributed Storage like Cassandra, Hadoop etc.</b><br/>
Odilon supports master-standby architecture for archival, backup and data protection, 
but it is not a Distributed Storage and it does not support active-active replication.
<br/>
<br/>
</li>
<li class="list-item"><b>Odilon is not a File System like GlusterFS, Ceph, ext4, etc.</b><br/>
It uses the underlying file system to stores objects as encrypted files, or in some configurations to break objects into chunks.
<br/>
<br/>
</li>
<li class="list-item"><b>Odilon is not a NoSQL database like MongoDB, CouchDB, etc.</b><br/> 
It does not use a database engine, 
Odilon uses its own journaling agent for Transaction Management 
and only supports very simple queries, ie. to retrieve an object and to list the objects of a bucket filtered by objectname's prefix.
<br/>
<br/>
</li>
<li class="list-item"><b>S3 compatibility</b><br/>
Odilon API is simpler than S3. It supports a subset of th the AWS S3 API, ie.the bucket/object methafor to organize the object space.
<br/>
<br/>
</li>
<li class="list-item"><b>Odilon is not optimized for a very large number of small files</b></b><br/>  
Odilon does not have optimization for lots of small files. 
The files are simply stored encrypted and compressed to local disks. 
Plus the extra meta file and shards for erasure coding.
<br/>
<br/>
</li>
</ul>

<h2>Who is using Odilon</h2>

<p>To be included in this list please send email to info@novamens.com</p>

<h3><a href="https://dellemuse.app" target="_blank">dellemuse</a></h3>
<p>Dellemuse is a service that enriches the cultural experience in museums and historical buildings through AI-generated audio guides that visitors can listen to on their mobile phones.
They use Odilon to store the audio files of the guides.</p>

<h3><a href="https://kbee.io" target="_blank">kbee</a></h3>
<p>Document management software. kbee uses Odilon to store technical and regulatory documentation in the aviation industry.</p>


				
<h2>Using Odilon</h2>
<p>
A Java client program that interacts with the Odilon server must include the Odilon SDK jar in the classpath.
A typical architecture for a Web Application is</p> 

<br/>
<br/>

![web-app-odilon-en](https://github.com/atolomei/odilon-server/assets/29349757/115e1cc0-223d-4f92-a121-e3f9ad3a1418)

<br/>
<br/>
Example to upload 2 pdf files:
<br/>
<br/>

```java
String endpoint = "http://localhost"; 

/** default port */
int port = 9234; 

/** default credentials */
String accessKey = "odilon";
String secretKey = "odilon";
			
String bucketName  = "demo_bucket";
String objectName1 = "demo_object1";
String objectName2 = "demo_object2";
			
File file1 = new File("test1.pdf");
File file2 = new File("test2.pdf");
			
/* put two objects in the bucket,
the bucket must exist before sending the object,
and object names must be unique for that bucket */
			
OdilonClient client = new ODClient(endpoint, port, accessKey, secretKey);

client.putObject(bucketName, objectName1, file1);
client.putObject(bucketName, objectName2, file2);
```
<p>
More info on the Odilon website <br/>
<a href="https://odilon.io/development.html" target="_blank">Java Application Development with Odilon</a>
</p>

<h2>Download</h2>
<p>
Current version of Odilon Server is <b>2.1</b>.
<ul>
<li><a href="https://odilon.io#download" target="_blank">Odilon Server</a></li>	
<li><a href="https://odilon.io#download" target="_blank">Odilon Java SDK</a></li>
<li><a href="https://mvnrepository.com/artifact/io.odilon/odilon-client" target="_blank">Odilon Java SDK. MVN Repository</a></li>		
</ul>
</p>

<h2>Installation and configuration</h2>
<p>
<ul>
<li><a href="https://odilon.io/configuration-linux.html" target="_blank">Installation, Configuration and Operation on Linux</a></li>	
<li><a href="https://odilon.io/configuration-mac.html" target="_blank">Installation, Configuration and Operation on Mac</a></li>	
<li><a href="https://odilon.io/configuration-windows.html" target="_blank">Installation, Configuration and Operation on Windows</a></li>




<li><a href="https://odilon.io/configuration-data-replication.html" target="_blank">Data Replication</a></li>
<li><a href="https://odilon.io/configuration-encryption.html" target="_blank">Encryption</a></li>
<li><a href="https://odilon.io/configuration-version-control.html" target="_blank">Version Control</a></li>
<li><a href="https://odilon.io/configuration-replication.html" target="_blank">Master-Standby replication</a></li>
<li><a href="https://odilon.io/configuration-https.html" target="_blank">Configuring HTTPS</a></li>		
</ul>
</p>

<h2>Java Application Development</h2>
<p>
<ul>
<li><a href="https://odilon.io/development.html" target="_blank">Java Application Development with Odilon</a></li>	
<li><a href="https://odilon.io/javadoc/1.14/index.html" target="_blank">Odilon SDK Javadoc</a></li>	
<li class="list-item"><a href="https://github.com/atolomei/odilon-client" target="_blank">odilon-client GitHub project</a></li>
<li class="list-item"><a href="https://github.com/atolomei/odilon-model" target="_blank">odilon-model GitHub project</a></li>
</ul>
</p>

<h2>Odilon Server Design</h2>
<p>
<ul>
<li><a href="https://odilon.io/architecture.html" target="_blank">Odilon Server Software Architecture</a></li>	
</ul>
</p>

<h2>Videos</h2>
<p>
<ul>
<li><a href="https://youtu.be/irR_Eeq3I-I?si=74nZnUh4mxNW3F7R" target="_blank">Odilon Installer for Windows - YouTube video (2 min)<a></li>	
<li><a href="https://youtu.be/eOdZ-6kn2CU?si=SMFz5BrrW18xfBcF" target="_blank">Odilon how to enable encryption - YouTube video (1 min)<a></li>	
<li><a href="https://youtu.be/kI6jG9vZAjI?si=3KSOpbvN-6ThJf1m" target="_blank">Odilon demo - YouTube video (4 min)<a></li>	
</ul>
</p>



