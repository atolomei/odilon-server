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

import java.io.InputStream;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.odilon.error.OdilonServerAPIException;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.log.Logger;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;
import io.odilon.security.AuthToken;
import io.odilon.security.TokenService;
import io.odilon.service.ServerSettings;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.XXTrafficControlService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.vfs.model.VFSObject;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * 
 * <p>Object's presigned url</p>
 * 
 * <p>A presigned URL is a way to grant temporary access to an Object, for example in an HTML webpage.
   It remains valid for a limited period of time which is specified when the URL is generated.</p>
 *  
 *   <ul>
 * 	 	<li>/presigned/object</li>
 *   </ul>
 *  
 *  @author atolomei@novamens.com (Alejandro Tolomei)
 *  
 */
@RestController
@RequestMapping(value = "/presigned/object")
public class ObjectControllerPresigned extends BaseApiController {
				
	static private Logger logger = Logger.getLogger(ObjectControllerPresigned.class.getName());
	
	@JsonIgnore
	@Autowired
	private TokenService tokenService;
	
	@JsonIgnore
	@Autowired
	ServerSettings serverSettings;
	
	public ObjectControllerPresigned(	ObjectStorageService objectStorageService, 
										VirtualFileSystemService virtualFileSystemService,
										SystemMonitorService monitoringService, 
										TrafficControlService trafficControlService,
										TokenService tokenService,
										ServerSettings serverSettings) {

		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
		this.tokenService=tokenService;
		this.serverSettings=serverSettings;
	}
	
	/**
	 * @param bucketName
	 * @param objectName
	 * @return
	 * @PathVariable("token") String stringToken
	 * 
	 */
	@RequestMapping(method = RequestMethod.GET)
	@ResponseBody
	public ResponseEntity<InputStreamResource> getPresignedObjectStream( @RequestParam("token") String stringToken ) {

		TrafficPass pass = null;
		
		try {
			
			pass = getTrafficControlService().getPass();
			
			if (stringToken==null)
				throw new OdilonServerAPIException("token is null");
									
			AuthToken authToken = this.tokenService.decrypt(stringToken);
			
			if (authToken==null)
				throw new OdilonServerAPIException("AuthToken is null");
			
			if (!authToken.isValid()) {
				logger.error(String.format("token expired -> t: %s", authToken.toString()));
				throw new OdilonServerAPIException(String.format("token expired -> t: %s", authToken.toString()));
			}
			
			String bucketName = authToken.bucketName; 
			String objectName = authToken.objectName;
			
			VFSObject object = getObjectStorageService().getObject(bucketName, objectName);
			
			if (object==null)
				throw new OdilonObjectNotFoundException(String.format("not found -> b: %s | o:%s",Optional.ofNullable(bucketName).orElse("null"),Optional.ofNullable(objectName).orElse("null")));
			
			HttpHeaders responseHeaders = new HttpHeaders();
			
			String f_name = object.getObjectMetadata().fileName.replace("[","").replace("]", "");
			responseHeaders.set("Content-Disposition", "inline; filename=\""+f_name +"\"");
			
			MediaType contentType = MediaType.valueOf(object.getObjectMetadata().contentType);
			
			if (object.getObjectMetadata().contentType()==null || object.getObjectMetadata().contentType.equals("application/octet-stream")) { 
					contentType = estimateContentType(f_name);
			}
			
			InputStream in = object.getInputStream();
		    
			getSystemMonitorService().getGetObjectMeter().mark();
			
			return ResponseEntity.ok()
			  .headers(responseHeaders)
		      .contentType(contentType)
		      .body(new InputStreamResource(in));
	    
		} catch (Exception e) {
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));			
		
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * @param f_name
	 * @return
	 */
	private MediaType estimateContentType(String f_name) {
		
		if (f_name==null)
			return MediaType.valueOf("application/octet-stream");
			
		if (isPdf(f_name))	
			return MediaType.valueOf("application/pdf");

		if (isAudio(f_name))	
			return MediaType.valueOf("audio/mpeg");

		if (isVideo(f_name))	
			return MediaType.valueOf("video/mpeg");
		
		if (isJpeg(f_name))
			return MediaType.valueOf("image/jpeg");
		
		if (isPng(f_name))
			return MediaType.valueOf("image/png");
		
		if (isGif(f_name))
			return MediaType.valueOf("image/gif");
		
		if (isWebp(f_name))	
			return MediaType.valueOf("image/svg+xml");
		
		if (isExcel(f_name))	
			return MediaType.valueOf("application/vnd.ms-excel");

		if (isWord(f_name))	
			return MediaType.valueOf("application/msword");
		
		if (isPowerPoint(f_name))	
			return MediaType.valueOf("application/vnd.ms-powerpoint");
		
		return MediaType.valueOf("application/octet-stream");
	}

	static public boolean isPowerPoint(String name) {
 		return name.toLowerCase().matches("^.*\\.(ppt|pptx)$"); 
	}
 	
	static public boolean isWord(String name) {
		return name.toLowerCase().matches("^.*\\.(doc|docx|rtf)$"); 
	}
	
	static public boolean isVideo(String filename) {
		return (filename.toLowerCase().matches("^.*\\.(mp4|flv|aac|ogg|wmv|3gp|avi|swf|svi|wtv|fla|mpeg|mpg|mov|m4v)$") ); 
	}
	
	static public boolean isAudio(String filename) {
		return filename.toLowerCase().matches("^.*\\.(mp3|wav|ogga|ogg|aac|m4a|m4a|aif|wma)$"); 
	}
	
	static public boolean isExcel(String name) {
		return name.toLowerCase().matches("^.*\\.(xls|xlsx|xlsm)$"); 
	}
	
	static public boolean isJpeg(String string) {
		return string.toLowerCase().matches("^.*\\.(jpg|jpeg)$"); 
	}
						
	static public boolean isPdf(String string) {
		return string.toLowerCase().matches("^.*\\.(pdf)$"); 
	}
	
	static public boolean isGif(String string) {
		return string.toLowerCase().matches("^.*\\.(gif)$"); 
	}
	
	static public boolean isWebp(String string) {
		return string.toLowerCase().matches("^.*\\.(webp)$"); 
	}
	
	static public boolean isPng(String string) {
		return string.toLowerCase().matches("^.*\\.(png)$"); 
	}
	
	static public boolean isGeneralImage(String string) {
		return string.toLowerCase().matches("^.*\\.(png|jpg|jpeg|gif|bmp|heic)$"); 
	}
	
	static public boolean isImage(String string) {
		return isGeneralImage(string) || string.toLowerCase().matches("^.*\\.(webp)$"); 
	}
}
