/*
 * Odilon Object Storage
 * (c) kbee 
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
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonServerAPIException;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;
import io.odilon.security.AuthToken;
import io.odilon.security.TokenService;
import io.odilon.service.ServerSettings;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.virtualFileSystem.model.VirtualFileSystemObject;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * API endopoint to get an Object's presigned url
 * </p>
 * 
 * <p>
 * A presigned URL is a way to grant temporary access to an Object, for example
 * in an HTML webpage. It remains valid for a limited period of time which is
 * specified when the URL is generated.
 * </p>
 * 
 * <ul>
 * <li>/presigned/object</li>
 * </ul>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
@RestController
@RequestMapping(value = "/presigned/object")
public class ObjectControllerPresigned extends BaseApiController {

    static private Logger logger = Logger.getLogger(ObjectControllerPresigned.class.getName());

    @JsonIgnore
    @Autowired
    private final TokenService tokenService;

    @JsonIgnore
    @Autowired
    private final ServerSettings serverSettings;

    public ObjectControllerPresigned(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService,
            SystemMonitorService monitoringService, TrafficControlService trafficControlService, TokenService tokenService,
            ServerSettings serverSettings) {

        super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
        this.tokenService = tokenService;
        this.serverSettings = serverSettings;
    }

     
    
    
    
    
    
    
    /**
     * @param bucketName
     * @param objectName
     * @return @PathVariable("token") String stringToken
     * 
     */
    
   /** 
    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<InputStreamResource> getPresignedObjectStream(@RequestParam("token") String stringToken) {

        TrafficPass pass = null;
        InputStream in = null;
        
        try {

            pass =  getTrafficControlService().getPass(this.getClass().getSimpleName());


            if (stringToken == null)
                throw new OdilonServerAPIException("token is null");

            AuthToken authToken = getTokenService().decrypt(stringToken);

            if (authToken == null)
                throw new OdilonServerAPIException(AuthToken.class.getSimpleName() + " is null");

            if (!authToken.isValid()) {
                logger.error(String.format("token expired -> t: %s", authToken.toString()));
                throw new OdilonServerAPIException(String.format("token expired -> t: %s", authToken.toString()));
            }

            String bucketName = authToken.getBucketName();
            String objectName = authToken.getObjectName();

            VirtualFileSystemObject object = getObjectStorageService().getObject(bucketName, objectName);

            if (object == null)
                throw new OdilonObjectNotFoundException(String.format("not found -> b: %s | o:%s",
                        Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));

            HttpHeaders responseHeaders = new HttpHeaders();

            String f_name = object.getObjectMetadata().getFileName().replace("[", "").replace("]", "");
            responseHeaders.set("Content-Disposition", "inline; filename=\"" + f_name + "\"");

            MediaType contentType = MediaType.valueOf(object.getObjectMetadata().getContentType());

            if (object.getObjectMetadata().contentType() == null
                    || object.getObjectMetadata().getContentType().equals("application/octet-stream")) {
                contentType = estimateContentType(f_name);
            }
            
            long length = object.getObjectMetadata().getLength();
            

            in = object.getInputStream();

            getSystemMonitorService().getGetObjectMeter().mark();

            int cacheDurationSecs = this.getServerSettings().getserverObjectstreamCacheSecs();

            if (authToken.getObjectCacheDurationSecs()>0)
            	cacheDurationSecs = authToken.getObjectCacheDurationSecs();
            else
            	cacheDurationSecs = this.getServerSettings().getserverObjectstreamCacheSecs();
            
           logger.debug("stream -> " + contentType + " | " + String.valueOf(length) +" bytes");
          
           responseHeaders.set(HttpHeaders.ACCEPT_RANGES, "bytes");
           
           return ResponseEntity.ok().
        		   cacheControl(CacheControl.maxAge(cacheDurationSecs, TimeUnit.SECONDS)).
        		   headers(responseHeaders).
        		   contentType(contentType).
        	       contentLength(length).      	
        		   body(new InputStreamResource(in));

 
           
        } catch (Exception e) {

        	if (in!=null) {
				try {
					in.close();
				} catch (IOException e1) {
					logger.error(e1, SharedConstant.NOT_THROWN);
				}
        	}
        	
            throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));

        } finally {
            getTrafficControlService().release(pass);
            mark();
        }
    }
*/
    
    
    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<InputStreamResource> getPresignedObjectStream(
            @RequestParam("token") String stringToken,
            @RequestHeader(value = "Range", required = false) String rangeHeader) {

        TrafficPass pass = null;
        InputStream in = null;

        try {
            pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

            if (stringToken == null)
                throw new OdilonServerAPIException("token is null");

            AuthToken authToken = getTokenService().decrypt(stringToken);

            if (authToken == null)
                throw new OdilonServerAPIException(AuthToken.class.getSimpleName() + " is null");

            if (!authToken.isValid()) {
                logger.error(String.format("token expired -> t: %s", authToken.toString()));
                throw new OdilonServerAPIException(String.format("token expired -> t: %s", authToken.toString()));
            }

            String bucketName = authToken.getBucketName();
            String objectName = authToken.getObjectName();

            VirtualFileSystemObject object = getObjectStorageService().getObject(bucketName, objectName);

            if (object == null)
                throw new OdilonObjectNotFoundException(String.format("not found -> b: %s | o:%s",
                        Optional.ofNullable(bucketName).orElse("null"),
                        Optional.ofNullable(objectName).orElse("null")));

            HttpHeaders responseHeaders = new HttpHeaders();
            String f_name = object.getObjectMetadata().getFileName().replace("[", "").replace("]", "");
            responseHeaders.set(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=\"" + f_name + "\"");

            
            
            if (object.getObjectMetadata().getFileName()!=null) {
				if (object.getObjectMetadata().getFileName().toLowerCase().endsWith(".svg")) {
					object.getObjectMetadata().setContentType("image/svg+xml");
				}
			}
            
            MediaType contentType = MediaType.valueOf(object.getObjectMetadata().getContentType());
            if (object.getObjectMetadata().contentType() == null
                    || object.getObjectMetadata().getContentType().equals("application/octet-stream")) {
                contentType = estimateContentType(f_name);
            }

            logger.debug(object.getObjectMetadata().getFileName()+ " "+ contentType.toString());
            
          
            
            long fileLength = object.getObjectMetadata().getLength();
            in = object.getInputStream();

            getSystemMonitorService().getGetObjectMeter().mark();

            int cacheDurationSecs = authToken.getObjectCacheDurationSecs() > 0
                    ? authToken.getObjectCacheDurationSecs()
                    : this.getServerSettings().getserverObjectstreamCacheSecs();

            responseHeaders.set(HttpHeaders.ACCEPT_RANGES, "bytes");

            // --- Manejo de Range ---
            if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
                // parsea "bytes=START-END"
                String[] ranges = rangeHeader.substring(6).split("-");
                long start = Long.parseLong(ranges[0]);
                long end = ranges.length > 1 && !ranges[1].isEmpty() ? Long.parseLong(ranges[1]) : fileLength - 1;
                long contentLength = end - start + 1;

                // skip hasta el inicio del rango
                in.skip(start);

                responseHeaders.setContentLength(contentLength);
                responseHeaders.add(HttpHeaders.CONTENT_RANGE,
                        "bytes " + start + "-" + end + "/" + fileLength);

                return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
                        .headers(responseHeaders)
                        .contentType(contentType)
                        .contentLength(contentLength) 
                        .cacheControl(CacheControl.maxAge(cacheDurationSecs, TimeUnit.SECONDS))
                        .body(new InputStreamResource(new LimitedInputStream(in, contentLength)));
            } else {
                // Full file
                responseHeaders.setContentLength(fileLength);
                return ResponseEntity.ok()
                        .headers(responseHeaders)
                        .contentType(contentType)
                        .contentLength(fileLength) 
                        .cacheControl(CacheControl.maxAge(cacheDurationSecs, TimeUnit.SECONDS))
                        .body(new InputStreamResource(in));
            }

        } catch (Exception e) {
            if (in != null) {
                try { 
                	in.close(); 
                } catch (IOException e1) { 
                	logger.error(e1, SharedConstant.NOT_THROWN); 
                }
            }
            throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR,
                    ErrorCode.INTERNAL_ERROR, getMessage(e));
        } finally {
            getTrafficControlService().release(pass);
            mark();
        }
    }
    
        
    public TokenService getTokenService() {
		return this.tokenService;
	}

	public ServerSettings getServerSettings() {
		return this.serverSettings;
	}

    
	
}
