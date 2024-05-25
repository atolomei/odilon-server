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
import java.util.List;
import java.util.Optional;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonServerAPIException;
import io.odilon.error.OdilonInternalErrorException;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;
import io.odilon.security.AuthToken;
import io.odilon.security.TokenService;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.vfs.model.VirtualFileSystemService;
/**
 * 
 * 	<ul>
 * 		<li>	/object/delete/{bucketName}/{objectName}</li>
		<li>	/object/deleteallpreviousversion/{bucketName}/{objectName}</li>
	 	<li>	/object/exists/{bucketName}/{objectName}</li>
		<li>	/object/get/{bucketName}/{objectName}</li>
		<li>	/object/getmetadata/{bucketName}/{objectName}</li>
		<li>	/object/getmetadatapreviousversion/{bucketName}/{objectName}</li>
		<li>	/object/getmetadatapreviousversionall/{bucketName}/{objectName}</li>
		<li>	/object/get/presignedurl/{bucketName}/{objectName}</li>
		<li>	/object/getpreviousversion/{bucketName}/{objectName}</li>
		<li>	/object/hasversions/{bucketName}/{objectName}</li>
		<li>	/object/upload/{bucketName}/{objectName}</li>
	</ul>

 *   @author atolomei@novamens.com (Alejandro Tolomei)
 *   
 */
@RestController
@RequestMapping(value = "/object")
public class ObjectController extends BaseApiController  {
		
		static private Logger logger = Logger.getLogger(ObjectController.class.getName());
		
		@JsonIgnore
		@Autowired
		private TokenService tokenService;
		
		public ObjectController(		ObjectStorageService objectStorageService, 
										VirtualFileSystemService virtualFileSystemService,
										SystemMonitorService monitoringService,
										TrafficControlService trafficControlService,
										TokenService tokenService) {

			super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
			this.tokenService=tokenService;
		}
		
		
		/**
		 * 
		 * 
		 */
		@RequestMapping(value = "/exists/{bucketName}/{objectName}", method = RequestMethod.GET)
		public ResponseEntity<Boolean> exists(	@PathVariable("bucketName") String bucketName, 
												@PathVariable("objectName") String objectName) {

			TrafficPass pass = null;
			
			try {
				pass = getTrafficControlService().getPass();
				
				if (!getObjectStorageService().existsBucket(bucketName))
					return new ResponseEntity<Boolean>(Boolean.valueOf(false), HttpStatus.OK);	

				return new ResponseEntity<Boolean>(Boolean.valueOf(getObjectStorageService().existsObject(bucketName, objectName) ? true : false), HttpStatus.OK);

			
			} catch (OdilonServerAPIException e) {
				throw e;
			} catch (Exception e) {
				throw new OdilonInternalErrorException(getMessage(e));
			}
			finally { 
				getTrafficControlService().release(pass);
				mark();
			}
		}

		/**
		 * 
		 */							
		@RequestMapping(value = "/hasversions/{bucketName}/{objectName}", method = RequestMethod.GET)
		public ResponseEntity<Boolean> hasVersions(	@PathVariable("bucketName") String bucketName, 
													@PathVariable("objectName") String objectName) {

			TrafficPass pass = null;
			
			try {

				pass = getTrafficControlService().getPass();

				if (!this.getVirtualFileSystemService().getServerSettings().isVersionControl())
					throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.API_NOT_ENABLED, "Version Control not enabled");
				
				
				if (!getObjectStorageService().existsObject(bucketName, objectName))
					throw new OdilonObjectNotFoundException(String.format("Object not Ffund -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
				
				return new ResponseEntity<Boolean>(Boolean.valueOf(getObjectStorageService().hasVersions(bucketName, objectName) ? true : false), HttpStatus.OK);
			
			} catch (OdilonServerAPIException e) {
				throw e;
			} catch (Exception e) {
				throw new OdilonInternalErrorException(getMessage(e));
			}
			finally { 
				getTrafficControlService().release(pass);
				mark();
			}
		}

		/**
		 * @param bucketName
		 * @param objectName
		 * @return
		 */
		@RequestMapping(path="/get/{bucketName}/{objectName}", method = RequestMethod.GET)
		@ResponseBody
		public ResponseEntity<InputStreamResource> getObjectStream(	@PathVariable("bucketName") String bucketName, 
																	@PathVariable("objectName") String objectName) {
			
			TrafficPass pass = null;
			
			try {

				pass = getTrafficControlService().getPass();
				
				if (!getObjectStorageService().existsObject(bucketName, objectName))
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s",Optional.ofNullable(bucketName).orElse("null"),Optional.ofNullable(objectName).orElse("null")));
				
				ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);
				
					if (meta==null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT)
						throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
				
				MediaType contentType=MediaType.APPLICATION_OCTET_STREAM;
				
				InputStream in = getObjectStorageService().getObjectStream(bucketName, objectName);
							
				getSystemMonitorService().getGetObjectMeter().mark();
				 
				 return ResponseEntity.ok()
			      .contentType(contentType)
			      .body(new InputStreamResource(in));
		    
			} catch (OdilonServerAPIException e1) {
				throw e1;
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));			
			
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}
		
		/**
		 * <p> Returns {@link InputStream} of the version passed as parameter. 
		 *  
		 *  It will return {@code null} if:
		 *  <ul> 
		 *  <li>version is non existent</li>
		 *  <li>previous versions were wiped</li>
		 *  </ul>
		 *  </p>
		 * 
		 * @param bucketName
		 * @param objectName
		 * 
		 * @return
		 */
		@RequestMapping(path="/getversion/{bucketName}/{objectName}", method = RequestMethod.GET)
		@ResponseBody
		public ResponseEntity<InputStreamResource> getObjectVersionStream(	@PathVariable("bucketName") String bucketName, 
																			@PathVariable("objectName") String objectName,
																			@RequestParam("version") Optional<Integer> version) {
			TrafficPass pass = null;
			
			try {

				pass = getTrafficControlService().getPass();
				
				if (!this.getVirtualFileSystemService().getServerSettings().isVersionControl())
					throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.API_NOT_ENABLED, "Version Control not enabled");

				
				if (!getObjectStorageService().existsObject(bucketName, objectName))
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
				
				ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);
				
				if (meta==null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT)
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
		
				if (version.isEmpty())
					throw new IllegalArgumentException("version can not be null");
				
				if (version.get()<0)
					throw new IllegalArgumentException("version must be 0 or greater");
				
				MediaType contentType=MediaType.APPLICATION_OCTET_STREAM;
				InputStream in = getObjectStorageService().getObjectPreviousVersionStream(bucketName, objectName, version.get().intValue());
				
				getSystemMonitorService().getGetObjectMeter().mark();
				
				 
				 return ResponseEntity.ok()
			      .contentType(contentType)
			      .body(new InputStreamResource(in));
		    
			} catch (OdilonServerAPIException e) {
				throw e;
				
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));			
			
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}

		/**
		 * @param bucketName
		 * @param objectName
		 * @return
		 */
		@RequestMapping(path="/getpreviousversion/{bucketName}/{objectName}", method = RequestMethod.GET)
		@ResponseBody
		public ResponseEntity<InputStreamResource> getObjectPreviousVersionStream(	@PathVariable("bucketName") String bucketName, 
																					@PathVariable("objectName") String objectName ) {
			TrafficPass pass = null;
			
			try {

				pass = getTrafficControlService().getPass();
				
				if (!this.getVirtualFileSystemService().getServerSettings().isVersionControl())
					throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.API_NOT_ENABLED, "Version Control not enabled");

				
				if (!getObjectStorageService().existsObject(bucketName, objectName))
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
				
				
				ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);
				
				if (meta==null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT)
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
				
				if (meta.version==0)
					throw new OdilonObjectNotFoundException(String.format("object version not found"));
				
				List<ObjectMetadata> list = getObjectStorageService().getObjectMetadataAllPreviousVersions(bucketName, objectName);
				
				if (list==null || list.isEmpty())
					throw new OdilonObjectNotFoundException(String.format("object version not found"));
					
				
				getSystemMonitorService().getGetObjectMeter().mark();

				MediaType contentType=MediaType.APPLICATION_OCTET_STREAM;
				InputStream in = getObjectStorageService().getObjectPreviousVersionStream(bucketName, objectName, list.get(list.size()-1).version);
				 
				return ResponseEntity.ok()
			     .contentType(contentType)
			     .body(new InputStreamResource(in));
		    
			} catch (OdilonServerAPIException e1) {
				logger.error(e1);
				throw e1;
				
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));			
			
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}
		
		/**
		 * @param bucketName
		 * @param objectName
		 * @return
		 */
		@RequestMapping(path="/get/presignedurl/{bucketName}/{objectName}", method = RequestMethod.GET)
		@ResponseBody																				
		public ResponseEntity<String> getPresignedUrl( @PathVariable("bucketName") String bucketName, 
													   @PathVariable("objectName") String objectName,
													   @RequestParam("durationSeconds") Optional<Integer> durationSeconds ) {
			
			TrafficPass pass = null;
			
			try {

				pass = getTrafficControlService().getPass();

				if (!getObjectStorageService().existsObject(bucketName, objectName))
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));

				ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);
				
				if (meta==null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT)
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));

				String token = durationSeconds.isPresent() ? this.tokenService.encrypt(new AuthToken(bucketName, objectName, durationSeconds.get())) : this.tokenService.encrypt(new AuthToken(bucketName, objectName));

				getSystemMonitorService().getGetObjectMeter().mark();
				
				return ResponseEntity.ok()
							.contentType(MediaType.APPLICATION_JSON)
							.body(token);
		    
			} catch (OdilonServerAPIException e) {
				throw e;
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));			
			
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}
		/**
		 * 
		 * @param bucketName
		 * @param objectName
		 * @return
		 */
		@RequestMapping(path="/getmetadata/{bucketName}/{objectName}", method = RequestMethod.GET)
		@ResponseBody						
		public ResponseEntity<ObjectMetadata> getObjectMetadata(	@PathVariable("bucketName") String bucketName, 
																	@PathVariable("objectName") String objectName) {
			
			TrafficPass pass = null;
			
			try {

				pass = getTrafficControlService().getPass();
				
				ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);
				
				if (meta==null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT)
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
				
				getSystemMonitorService().getGetObjectMeter().mark();
				
				return ResponseEntity.ok()
					      .contentType(MediaType.APPLICATION_JSON)
					      .body(meta);
		    
			
			} catch (OdilonServerAPIException e) {
				throw e;
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));			
			
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}
		/**
		 * 
		 * @param bucketName
		 * @param objectName
		 * @return
		 * 
		 */
		@RequestMapping(path="/getmetadatapreviousversion/{bucketName}/{objectName}", method = RequestMethod.GET)
		@ResponseBody						
		public ResponseEntity<ObjectMetadata> getObjectMetadataPreviousVersion(	@PathVariable("bucketName") String bucketName, 
																				@PathVariable("objectName") String objectName) {
			
			TrafficPass pass = null;
			
			try {

				pass = getTrafficControlService().getPass();
				
				if (!this.getVirtualFileSystemService().getServerSettings().isVersionControl())
					throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.API_NOT_ENABLED, "Version Control not enabled");
				
				ObjectMetadata meta = getObjectStorageService().getObjectMetadataPreviousVersion(bucketName, objectName);

				if (meta==null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT)
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
				
				getSystemMonitorService().getGetObjectMeter().mark();

				
				return ResponseEntity.ok()
					      .contentType(MediaType.APPLICATION_JSON)
					      .body(meta);
			
			} catch (OdilonServerAPIException e) {
				throw e;
				
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));			
			
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}


		/**
		 * @param bucketName
		 * @param objectName
		 * @return
		 */
		@RequestMapping(path="/getmetadatapreviousversionall/{bucketName}/{objectName}", method = RequestMethod.GET)
		@ResponseBody						
		public ResponseEntity<List<ObjectMetadata>> getObjectMetadataAllPreviousVersion(	@PathVariable("bucketName") String bucketName, 
																							@PathVariable("objectName") String objectName) {
			
			TrafficPass pass = null;
			
			try {

				pass = getTrafficControlService().getPass();
				
				if (!this.getVirtualFileSystemService().getServerSettings().isVersionControl())
					throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.API_NOT_ENABLED, "Version Control not enabled");
				
				ObjectMetadata meta = getObjectStorageService().getObjectMetadataPreviousVersion(bucketName, objectName);
				
				if (meta==null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT)
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));

				List<ObjectMetadata> list = getObjectStorageService().getObjectMetadataAllPreviousVersions(bucketName, objectName);

				getSystemMonitorService().getGetObjectMeter().mark();

				
				return new ResponseEntity<List<ObjectMetadata>>(list, HttpStatus.OK);

			} catch (OdilonServerAPIException e) {
				throw e;
			} catch (Exception e) {
				logger.error(e);
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));			
			
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}

		/**
		 * @param bucketName
		 * @param objectName
		 */
		@RequestMapping(path="/delete/{bucketName}/{objectName}", method = RequestMethod.DELETE)
		public void deleteObject(	@PathVariable("bucketName") String bucketName, 
									@PathVariable("objectName") String objectName) {
			
			TrafficPass pass = null;
			try {
				
				pass = getTrafficControlService().getPass();
				
				if (!getObjectStorageService().existsObject(bucketName, objectName))
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
				
				getObjectStorageService().deleteObject(bucketName, objectName);
				getSystemMonitorService().getDeleteObjectCounter().inc();

				
			} catch (OdilonServerAPIException e) {
				throw e;
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}
		
		/**
		 * @param bucketName
		 * @param objectName
		 */
		@RequestMapping(path="/deleteallpreviousversion/{bucketName}/{objectName}", method = RequestMethod.DELETE)
		public void deleteObjectAllPreviousVersion(	@PathVariable("bucketName") String bucketName, 
													@PathVariable("objectName") String objectName) {
			
			TrafficPass pass = null;
			try {
				
				pass = getTrafficControlService().getPass();
				
				if (!this.getVirtualFileSystemService().getServerSettings().isVersionControl())
					throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.API_NOT_ENABLED, "Version Control not enabled");
				
				ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName); 
				getObjectStorageService().deleteObjectAllPreviousVersions(meta);
				
				getSystemMonitorService().getObjectDeleteAllVersionsCounter().inc();
				
				
			} catch (OdilonServerAPIException e) {
				throw e;
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
			} finally {
				if (pass!=null)
					getTrafficControlService().release(pass);
				mark();
			}
		}


		/**
		 * @param bucketName
		 * @param objectName
		 */					
		@RequestMapping(path="/restorepreviousversion/{bucketName}/{objectName}", method = RequestMethod.POST)
		public void restorePreviousVersion(	@PathVariable("bucketName") String bucketName, 
											@PathVariable("objectName") String objectName) {
			
			TrafficPass pass = null;

			try {
				
				pass = getTrafficControlService().getPass();

				if (!this.getVirtualFileSystemService().getServerSettings().isVersionControl())
					throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.API_NOT_ENABLED, "Version Control not enabled");
				
				if (!getObjectStorageService().existsObject(bucketName, objectName))
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));
				
				
				ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);
				
				if (meta==null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT)
					throw new OdilonObjectNotFoundException(String.format("object not found -> b: %s | o:%s", Optional.ofNullable(bucketName).orElse("null"), Optional.ofNullable(objectName).orElse("null")));

				if (meta.version==0)
					throw new OdilonObjectNotFoundException(String.format("Object has no previous version -> b:" + bucketName + " o:"+objectName));
				

				getObjectStorageService().restorePreviousVersion(bucketName, objectName);

				getSystemMonitorService().getObjectRestorePreviousVersionCounter().inc();
				
				
				
				
			} catch (OdilonServerAPIException e) {
				throw e;
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}

		
		/**
		 * @param bucketName
		 * @param objectName
		 * @param fileName
		 * @param file
		 * @return
		 * 
		 * @RequestParam("fileName") String fileName,
		 * @RequestHeader("Content-Type") String contentType,
		 */
		@PostMapping(path="/upload/{bucketName}/{objectName}")
		@ResponseBody
		public ResponseEntity <ObjectMetadata> putObject(		
				@PathVariable("bucketName") String bucketName, 
				@PathVariable("objectName") String objectName,
				@RequestParam("file") MultipartFile file,
				@RequestParam("fileName") Optional<String> oFileName,
				@RequestParam("Content-Type") String contentType,
				@RequestParam("version") Optional<Integer> version
																												
				) {

			TrafficPass pass = null;
			
			try {
				
				pass = getTrafficControlService().getPass();
				
				String fileName = Optional.ofNullable(oFileName.get()).orElseGet(() -> objectName);
				
				ObjectMetadata meta;
				
				if (version.isEmpty()) {
					getObjectStorageService().putObject(bucketName, objectName, file.getInputStream(), fileName, contentType);
					meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);					
				}
				else {
					meta = getObjectStorageService().getObjectMetadataPreviousVersion(bucketName, objectName, version.get().intValue());
					
					if (meta!=null) {
						logger.debug("version not done");
					}
				}
				
				getSystemMonitorService().getPutObjectMeter().mark();
				
				return ResponseEntity.ok()
					      .contentType(MediaType.APPLICATION_JSON)
					      .body(meta);
				
			} catch (IllegalStateException e) {
				throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.DATA_STORAGE_MODE_OPERATION_NOT_ALLOWED, getMessage(e));
				
			} catch (OdilonServerAPIException e) {
				throw e;
			} catch (Exception e) {
				throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
				
			} finally {
				getTrafficControlService().release(pass);
				mark();
			}
		}
		
		@PostConstruct
		public void init() {
		}
		
}
