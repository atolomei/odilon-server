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
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
 
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonServerAPIException;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;
 
import io.odilon.security.TokenService;
import io.odilon.service.ServerSettings;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.virtualFileSystem.model.VirtualFileSystemObject;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * API endopoint to get an Object's permanent, public url
 * </p>
 * 
 * <p>
 * Url permanente para acceso publico a los objectos, para http cache y CDN.
 * ObjectMetadata debe tener publicAccess = true * 
 * </p>
 * 
 * <ul>
 * <li>/public/${bucketName}/${objectName}</li>
 * </ul>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
@RestController
@RequestMapping(value = "/public")
public class ObjectControllerPublic extends BaseApiController {

    static private Logger logger = Logger.getLogger(ObjectControllerPublic.class.getName());

    @JsonIgnore
    @Autowired
    private final TokenService tokenService;

    @JsonIgnore
    @Autowired
    private final ServerSettings serverSettings;

    public ObjectControllerPublic(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService,
            SystemMonitorService monitoringService, TrafficControlService trafficControlService, TokenService tokenService,
            ServerSettings serverSettings) {

        super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
        this.tokenService = tokenService;
        this.serverSettings = serverSettings;
    }

    
    /**
	 * @param bucketName
	 * @param objectName
	 * @return
	 */
	@RequestMapping(path = "/{bucketName}/{objectName}/{fileName}", method = RequestMethod.GET)
	@ResponseBody
	public ResponseEntity<InputStreamResource> getObjectStream(
			@PathVariable("bucketName") String bucketName, 
			@PathVariable("objectName") String objectName, 
			@PathVariable("fileName") String fileName,
			@RequestHeader(value = "Range", required = false) String rangeHeader) {

        TrafficPass pass = null;
        InputStream in = null;

        try {
            pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

            VirtualFileSystemObject object = getObjectStorageService().getObject(bucketName, objectName);

            if (object == null)
                throw new OdilonObjectNotFoundException(String.format("not found -> b: %s | o:%s",
                		Optional.ofNullable(bucketName).orElse("null"),
                        Optional.ofNullable(objectName).orElse("null")));

            ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);

            logger.debug(meta.getFileName()+ "  -> "+  (meta.isPublicAccess() ? "public": "not public"));

            if (!meta.isPublicAccess()) {
                logger.error(meta.getFileName() + "  -> "+  (meta.isPublicAccess() ? "public": "not public"));
            	throw new OdilonServerAPIException(ODHttpStatus.UNAUTHORIZED, ErrorCode.AUTHENTICATION_ERROR, meta.toString());
            }
            
            HttpHeaders responseHeaders = new HttpHeaders();
            String f_name = meta.getFileName().replace("[", "").replace("]", "");
            responseHeaders.set(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=\"" + f_name + "\"");

            if (meta.getFileName()!=null) {
				if (meta.getFileName().toLowerCase().endsWith(".svg")) {
					meta.setContentType("image/svg+xml");
				}
			}
            
            MediaType contentType = MediaType.valueOf(meta.getContentType());
            if (meta.contentType() == null || meta.getContentType().equals("application/octet-stream")) {
                contentType = estimateContentType(f_name);
            }


            long fileLength = getSrcFileLength(meta);
            
            in = object.getInputStream();


            // CACHE
            String cacheHeader = "public, max-age=" + String.valueOf(ServerConstant.ONE_YEAR_SECS)+ ", immutable, no-transform";
            
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
           
                responseHeaders.set(HttpHeaders.CACHE_CONTROL, cacheHeader);

                // ETAG
                responseHeaders.setETag("\"" + fileLength + "\"");

                // LAST MODIFIED (si tenés timestamp)
                OffsetDateTime lastModified = meta.getLastModified();
                if (lastModified != null) {
                    responseHeaders.setLastModified(lastModified.toInstant().toEpochMilli());
                }
                
                // SECURITY
                responseHeaders.set("X-Content-Type-Options", "nosniff");

                // CORS (si aplica)
                responseHeaders.set("Access-Control-Allow-Origin", "*");
                responseHeaders.set("Access-Control-Expose-Headers",
                    "Content-Length, Content-Range, Accept-Ranges");

                getSystemMonitorService().getGetObjectMeter().mark();
                
                return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
                        .headers(responseHeaders)
                        .contentType(contentType)
                        .contentLength(contentLength) 
                        .cacheControl(
                        	    CacheControl.maxAge(ServerConstant.ONE_YEAR_SECS, TimeUnit.SECONDS)
                        	        .cachePublic()
                        	        .immutable()
                        	)
                        .body(new InputStreamResource(new LimitedInputStream(in, contentLength)));
            } else {
                // Full file
                responseHeaders.setContentLength(fileLength);
                responseHeaders.set(HttpHeaders.CACHE_CONTROL, cacheHeader);

                // ETAG
                responseHeaders.setETag("\"" + fileLength + "\"");
              
                // LAST MODIFIED (si tenés timestamp)
                OffsetDateTime lastModified = meta.getLastModified();
                if (lastModified != null) {
                    responseHeaders.setLastModified(lastModified.toInstant().toEpochMilli());
                }
       
                // SECURITY
                responseHeaders.set("X-Content-Type-Options", "nosniff");

                // CORS (si aplica)
                responseHeaders.set("Access-Control-Allow-Origin", "*");
                responseHeaders.set("Access-Control-Expose-Headers",
                    "Content-Length, Content-Range, Accept-Ranges");
                
                
                if (fileLength==-1) {
                    return ResponseEntity.ok()
	                        .headers(responseHeaders)
	                        .contentType(contentType)
	                        .cacheControl(
	                        	    CacheControl.maxAge(ServerConstant.ONE_YEAR_SECS, TimeUnit.SECONDS)
	                        	        .cachePublic()
	                        	        .immutable()
	                        	)
	                        .body(new InputStreamResource(in));
	            }
                else {
	                return ResponseEntity.ok()
	                        .headers(responseHeaders)
	                        .contentType(contentType)
	                        .contentLength(fileLength) 
	                        .cacheControl(
	                        	    CacheControl.maxAge(ServerConstant.ONE_YEAR_SECS, TimeUnit.SECONDS)
	                        	        .cachePublic()
	                        	        .immutable()
	                        	)
	                        .body(new InputStreamResource(in));
	                }
                }

        } catch (OdilonServerAPIException e) {
        	logger.error(e);
        	if (in != null) {
                 try { 
                 	in.close(); 
                 } catch (IOException e1) { 
                 	logger.error(e1, SharedConstant.NOT_THROWN); 
                 }
             }
        	throw e;
        	
        } catch (Exception e) {
        	logger.error(e);
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
