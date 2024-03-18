/*
 * Odilon Object Storage
 * (C) Novamens 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.odilon;

import javax.annotation.PostConstruct;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;

/**
 * 
 * <p><a href="https://odilon.io">Odilon </a> is a scalable and lightweight Open Source Object Storage that runs on standard hardware.</p>
 * <p>It is an infrastructure software designed to be used by applications that need to store terabytes 
 * of medium to large size objects (photos, pdfs, audio, video...) securely and safely through 
 * encryption, replication and redundancy.</p>
 * 
 * <p>It has a simple single-level folder structure similar to the Bucket / Object model of <a href="https://aws.amazon.com/en/s3/"> Amazon S3</a>. 
 * It is small and easy to integrate, offers encryption, data protection and fault tolerance 
 * (software RAID and Erasure Codes) and detection of silent data degradation. Odilon also supports 
 * version control and master - standby replication over the Internet for disaster recovery and 
 * ransomware protection.</p>
 * 
 * <b>Architecture</b><br/>
 * 
 * <p>Odilon has 3 layers 
 * 
 * <ul>
 *   <li>
 *   <b>API</b><br/>
 *   	RESTFul API implemented with Spring Boot<br/>
 *   	API Controllers:  Bucket CRUD, Object CRUD, System info,<br/>
 *   	API controllers interact with the {@link ObjectStorageService}, they do not	see the lower layers.  
 *   <br/>
 *   </li>
 *   <li>
 *   <b>Object Storage Service</b><br/>
 *   Buckets and Objects<br/>
 *   It uses a Virtual File System that supports redundancy, bit rot detection and error correction 
 *   <br/>   
 *   <br/>
 *   </li>
 *   <li>
 *   <b>Virtual File System</b><br/>
 *   RAID 0
 *   RAID 1
 *   RAID 6 
 *   Supports at rest encryption 
 *   redundancy, error detection and error correction (RAID) 
 *   it uses RAID drivers for I/O on the underlying {@code Drive} 
 *   
 *   <br/>
 *   <br/>
 *   </li>
 * </ul>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@SpringBootApplication
@ComponentScan({"io.odilon"})
public class OdilonApplication {
						
	static private Logger std_logger = Logger.getLogger("StartupLogger");
			
	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(OdilonApplication.class.getName());

	static public String[] cmdArgs = null;
	
	public static void main(String[] args) {
		SpringApplication application = new SpringApplication(OdilonApplication.class);
		application.setBannerMode(Banner.Mode.OFF);
		cmdArgs = args;
		application.run(args);
	}
	
	public OdilonApplication() {
	}
	
	@PostConstruct
	public void onInitialize() {
		
		std_logger.info("");
		for (String s : OdilonVersion.getAppCharacterName())
			std_logger.info(s);
		
		std_logger.info(ServerConstant.SEPARATOR);
		std_logger.info("This software is licensed under the Apache License, Version 2.0");
		std_logger.info("http://www.apache.org/licenses/LICENSE-2.0");

		initShutdownMessage();
	}
	
	/**
	 *
	 *
	 */
	private void initShutdownMessage() {
	    Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
            	std_logger.info("");
            	std_logger.info("As the roman legionaries used to say when falling in battle");
            	std_logger.info("'Dulce et decorum est pro patria mori'...Shuting down... goodbye.");
            	std_logger.info("");
            }
        });
	}
}

