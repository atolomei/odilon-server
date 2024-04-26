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


import jakarta.annotation.PostConstruct;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;

/**
 * 
 * 
 * <p>Odilon has three hierarchical layers (API, Object Storage, Virtual File System) and about a dozen general services used by them (Scheduler, Lock, Journal, Cache, Encryption, etc.).
 *  
 * See Article: <b><a href="https://odilon.io/architecture.html">Odilon Architecture</a></b></p>
 * 
 * <ul>
 *   <li>
 *   <b>API</b><br/>
 *   <p>This is the HTTP/S interface used by client applications, like Odilon Java SDK, it is a RESTFul API implemented with Spring Boot: Bucket and Object operations, System info, and others. They interact directly with the Object Storage, they do not see the lower layers.
 *   	API Controllers:  Bucket CRUD, Object CRUD, System info,<br/>
 *   	API controllers interact with the {@link ObjectStorageService}, they do not	see the lower layers.
 *   </p>  
 *   </li>
 *   <li>
 *   <b>Object Storage Service</b><br/>
 *   It is essentially an intermediary that downloads the requirements into the Virtual File System
 *   </li>
 *   
 *   <li>
 *   <b>Virtual File System</b><br/>
     *The Virtual File System layer manages the repository on top of the OS File System. Odilon uses the underlying File System to store objects as encrypted files, or in some configurations to break objects into chunks. It implements software RAID, which depending on the configuration can be RAID 0, RAID 1, RAID 6/Erasure Coding.
 *	  Odilon uses Reed Solomon encoding for Erasure Codes. 
 *   it uses RAID drivers for I/O on the underlying {@code Drive} 
 *   <br/>
 *   <br/>
 *   </li>
 * </ul>
 * 
 * @see {@link OdilonVersion#VERSION} for the version of the server
 * 
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
	
	
	/**
	 @Bean
	 public FilterRegistrationBean<Filter> filterRegistrationBean() {
	        CharacterEncodingFilter filter = new CharacterEncodingFilter();
	        filter.setEncoding("UTF-8");
	        filter.setForceEncoding(true);

	        FilterRegistrationBean<Filter> registrationBean = new FilterRegistrationBean<Filter>();
	        registrationBean.setFilter(filter);
	        registrationBean.addUrlPatterns("/*");
	        return registrationBean;
	    }
	 **/

}

