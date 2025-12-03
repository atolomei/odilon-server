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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.odilon.error.OdilonServerAPIException;
import io.odilon.error.OdilonInternalErrorException;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.log.Logger;
import io.odilon.model.Bucket;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.list.Item;
import io.odilon.model.list.DataList;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Bucket API
 * </p>
 * <ul>
 * <li>/bucket/create/{name}</li>
 * <li>/bucket/delete/{name}</li>
 * <li>/bucket/exists/{name}</li>
 * <li>/bucket/forcedelete/{name}</li>
 * <li>/bucket/get/{name}</li>
 * <li>/bucket/list</li>
 * </ul>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@RestController
@RequestMapping(value = "/bucket")
public class BucketController extends BaseApiController {

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(BucketController.class.getName());

	@Autowired
	public BucketController(ObjectStorageService objectStorageService,
			VirtualFileSystemService virtualFileSystemService, SystemMonitorService monitoringService,
			TrafficControlService trafficControlService) {
		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
	}

	/**
	 * <p>
	 * List buckets in JSON format
	 * </p>
	 */
	@RequestMapping(value = "/list", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<List<Bucket>> getBuckets() {

		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());
			List<Bucket> list = new ArrayList<Bucket>();
			getObjectStorageService().findAllBuckets().forEach(item -> list.add(new Bucket(item.getName(), item.getId(),
					item.getCreationDate(), item.getLastModifiedDate(), item.getStatus())));
			return new ResponseEntity<List<Bucket>>(list, HttpStatus.OK);

		} catch (OdilonInternalErrorException e) {
			throw e;

		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * 
	 * @param bucketName
	 * @return
	 */
	@RequestMapping(value = "/objects/{name}", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<DataList<Item<ObjectMetadata>>> queryObjects(@PathVariable("name") String bucketName,
			@RequestParam("offset") Optional<Long> offset, @RequestParam("pageSize") Optional<Integer> pageSize,
			@RequestParam("prefix") Optional<String> prefix,
			@RequestParam("serverAgentId") Optional<String> serverAgentId) {

		TrafficPass pass = null;

		try {

			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			DataList<Item<ObjectMetadata>> result = getObjectStorageService().listObjects(bucketName, offset, pageSize,
					prefix, serverAgentId);
			return new ResponseEntity<DataList<Item<ObjectMetadata>>>(result, HttpStatus.OK);

		} catch (OdilonInternalErrorException e) {
			throw e;

		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	// =====================================================================

	/**
	 * <p>
	 * Get a Bucket in JSON format
	 * </p>
	 * The server manages Buckets as instances of the class {@link ServerBucket} but
	 * buckets are exported to the clients as {@link Bucket}
	 * 
	 */
	@RequestMapping(value = "/get/{name}", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<Bucket> get(@PathVariable("name") String name) {

		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			ServerBucket bucket = getObjectStorageService().findBucketName(name);

			if (bucket == null)
				throw new OdilonObjectNotFoundException(ErrorCode.BUCKET_NOT_EXISTS,
						String.format("bucket does not exist -> %s", name));

			return new ResponseEntity<Bucket>(new Bucket(bucket.getName(), bucket.getId(), bucket.getCreationDate(),
					bucket.getLastModifiedDate(), bucket.getStatus()), HttpStatus.OK);

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * 
	 * 
	 */
	@RequestMapping(value = "/exists/{name}", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<Boolean> exists(@PathVariable("name") String name) {
		TrafficPass pass = null;
		try {
			
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			return new ResponseEntity<Boolean>(
					Boolean.valueOf(getObjectStorageService().existsBucket(name) ? true : false), HttpStatus.OK);

		} catch (OdilonServerAPIException e) {
			throw e;

		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * 
	 */
	@RequestMapping(value = "/isempty/{name}", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<Boolean> isEmpty(@PathVariable("name") String name) {
		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());


			ServerBucket bucket = getObjectStorageService().findBucketName(name);

			if (bucket == null)
				throw new OdilonObjectNotFoundException(ErrorCode.BUCKET_NOT_EXISTS,
						String.format("bucket does not exist -> %s", name));

			return new ResponseEntity<Boolean>(
					Boolean.valueOf(getObjectStorageService().isEmptyBucket(name) ? true : false), HttpStatus.OK);

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * <p>
	 * </p>
	 * 
	 * @param name
	 * @return
	 */
	@RequestMapping(value = "/create/{name}", produces = "application/json", method = RequestMethod.POST)
	public void create(@PathVariable("name") String name) {

		TrafficPass pass = null;

		try {

			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());


			if (getObjectStorageService().existsBucket(name))
				throw new OdilonServerAPIException(ODHttpStatus.CONFLICT, ErrorCode.OBJECT_ALREADY_EXIST,
						String.format("bucket already exist -> %s", Optional.ofNullable(name).orElse("null")));

			getObjectStorageService().createBucket(name);

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * <p>
	 * Rename a Bucket and returns the Bucket in JSON format
	 * </p>
	 */
	@RequestMapping(value = "/rename/{name}/{newname}", produces = "application/json", method = RequestMethod.POST)
	public ResponseEntity<Bucket> rename(@PathVariable("name") String name, @PathVariable("newname") String newname) {

		TrafficPass pass = null;

		try {

			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());


			ServerBucket bucket = getObjectStorageService().findBucketName(name);

			if (bucket == null)
				throw new OdilonObjectNotFoundException(ErrorCode.BUCKET_NOT_EXISTS,
						String.format("bucket does not exist -> %s", name));

			if (getObjectStorageService().existsBucket(newname)) {
				throw new OdilonServerAPIException(ODHttpStatus.CONFLICT, ErrorCode.OBJECT_ALREADY_EXIST, String
						.format("new bucket name already exist -> %s", Optional.ofNullable(newname).orElse("null")));
			}

			bucket = getObjectStorageService().updateBucketName(bucket, newname);

			return new ResponseEntity<Bucket>(new Bucket(bucket.getName(), bucket.getId(), bucket.getCreationDate(),
					bucket.getLastModifiedDate(), bucket.getStatus()), HttpStatus.OK);

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * 
	 */
	@RequestMapping(value = "/delete/{name}", produces = "application/json", method = RequestMethod.DELETE)
	public void delete(@PathVariable("name") String name) {
		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());


			if (getObjectStorageService().existsBucket(name)) {
				if (getObjectStorageService().isEmptyBucket(name)) {
					getObjectStorageService().deleteBucketByName(name);
				} else
					throw new OdilonServerAPIException(ODHttpStatus.CONFLICT, ErrorCode.BUCKET_NOT_EMPTY,
							String.format("bucket is not empty -> %s", Optional.ofNullable(name).orElse("null")));
			} else {
				throw new OdilonObjectNotFoundException(ErrorCode.BUCKET_NOT_EXISTS,
						String.format("bucket does not exist -> %s", Optional.ofNullable(name).orElse("null")));
			}
		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	@RequestMapping(value = "/forcedelete/{name}", produces = "application/json", method = RequestMethod.DELETE)
	public void forceDelete(@PathVariable("name") String name) {

		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (getObjectStorageService().existsBucket(name)) {
				getObjectStorageService().deleteBucketByName(name);
			} else
				throw new OdilonObjectNotFoundException(ErrorCode.BUCKET_NOT_EXISTS,
						String.format("bucket does not exist -> %s", Optional.ofNullable(name).orElse("null")));

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	@RequestMapping(value = "/deleteallpreviousversion/{name}", produces = "application/json", method = RequestMethod.DELETE)
	public ResponseEntity<Boolean> deleteAllPreviousVersions(@PathVariable("name") String name) {

		TrafficPass pass = null;

		try {

			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());


			if (!this.getVirtualFileSystemService().getServerSettings().isVersionControl())
				throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.API_NOT_ENABLED,
						"Version Control not enabled");

			if (!getObjectStorageService().existsBucket(name))
				throw new OdilonObjectNotFoundException(ErrorCode.BUCKET_NOT_EXISTS,
						String.format("bucket does not exist -> %s", Optional.ofNullable(name).orElse("null")));

			if (!getObjectStorageService().isEmptyBucket(name))
				getObjectStorageService().deleteBucketAllPreviousVersions(name);

			return new ResponseEntity<Boolean>(Boolean.valueOf(true), HttpStatus.OK);

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	@PostConstruct
	public void init() {
	}

}
