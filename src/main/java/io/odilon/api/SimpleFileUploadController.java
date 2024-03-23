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
package io.odilon.api;

import java.io.IOException;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import io.odilon.log.Logger;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.util.RandomIDGenerator;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@RestController
@RequestMapping(value = "/dev")
public class SimpleFileUploadController extends BaseApiController {

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(SimpleFileUploadController.class.getName());
	
	public SimpleFileUploadController(		ObjectStorageService objectStorageService, 
									VirtualFileSystemService virtualFileSystemService,
									SystemMonitorService monitoringService, 
									TrafficControlService trafficControlService) {
		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
		
	}
	
	public class FileUploadResponse {
		
	    private String fileName;
	    private String downloadUri;
	    private long size;
	    
		public String getFileName() {
			return fileName;
		}
		public void setFileName(String fileName) {
			this.fileName = fileName;
		}
		public String getDownloadUri() {
			return downloadUri;
		}
		public void setDownloadUri(String downloadUri) {
			this.downloadUri = downloadUri;
		}
		public long getSize() {
			return size;
		}
		public void setSize(long size) {
			this.size = size;
		}
	}
	
	 @RequestMapping(value = "/upload", produces = "application/json", method = RequestMethod.POST)
	 public ResponseEntity<FileUploadResponse> uploadFile(@RequestParam("file") MultipartFile multipartFile) throws IOException {
	         
		 try {
	        String fileName = StringUtils.cleanPath(multipartFile.getOriginalFilename());
	        long size = multipartFile.getSize();
	         
	        String filecode =  saveFile(fileName, multipartFile);
	         
	        FileUploadResponse response = new FileUploadResponse();
	        response.setFileName(fileName);
	        response.setSize(size);
	        response.setDownloadUri("/downloadFile/" + filecode);
	        
	        return new ResponseEntity<>(response, HttpStatus.OK);
		 }
	        finally {
	        	mark();
	        }
	        
	    }
	 
	 RandomIDGenerator idGenerator = new RandomIDGenerator();
	 	
	 private String saveFile(String fileName, MultipartFile multipartFile) throws IOException {
		        
			Path uploadPath = Paths.get("c:"+File.separator+"temp"+File.separator+"odilon-upload");
		          
		        if (!Files.exists(uploadPath)) {
		            Files.createDirectories(uploadPath);
		        }
		 
		        String fileCode = idGenerator.randomString(8);
		         
		        try (InputStream inputStream = multipartFile.getInputStream()) {
		            Path filePath = uploadPath.resolve(fileCode + "-" + fileName);
		            Files.copy(inputStream, filePath, StandardCopyOption.REPLACE_EXISTING);
		        } catch (IOException ioe) {       
		            throw new IOException("Could not save file: " + fileName, ioe);
		        }
		         
		        return fileCode;
	}
	
}
