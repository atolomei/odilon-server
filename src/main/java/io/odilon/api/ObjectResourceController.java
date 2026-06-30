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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonInternalErrorException;
import io.odilon.error.OdilonObjectNotFoundException;
import io.odilon.error.OdilonServerAPIException;
import io.odilon.log.Logger;
import io.odilon.model.Bucket;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.net.ErrorCode;
import io.odilon.net.ODHttpStatus;
import io.odilon.service.ObjectStorageService;
import io.odilon.service.ServerSettings;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * <p>
 * Resource-oriented REST API. Fully additive — no existing {@code /object/*} or
 * {@code /bucket/*} endpoint is modified or removed. The existing SDK and all
 * legacy clients continue to work unchanged.
 * </p>
 *
 * <h3>Bucket resources</h3>
 * 
 * <pre>
 * GET    /buckets                           list all buckets
 * PUT    /buckets/{bucket}                  create bucket (idempotent: 201 or 200)
 * DELETE /buckets/{bucket}                  delete empty bucket → 204
 * HEAD   /buckets/{bucket}                  check bucket exists → 200 or 404
 * </pre>
 *
 * <h3>Object resources</h3>
 * 
 * <pre>
 * GET    /buckets/{bucket}/objects                          list objects (paged)
 * GET    /buckets/{bucket}/objects/{object}                 download object binary
 * PUT    /buckets/{bucket}/objects/{object}                 create or replace object (upsert)
 * PATCH  /buckets/{bucket}/objects/{object}                 update object metadata only (no re-upload)
 * DELETE /buckets/{bucket}/objects/{object}                 delete object → 204
 * HEAD   /buckets/{bucket}/objects/{object}                 object metadata as response headers
 * GET    /buckets/{bucket}/objects/{object}/versions        list all previous versions
 * GET    /buckets/{bucket}/objects/{object}/versions/{ver}  download a specific version
 * POST   /buckets/{bucket}/objects/{object}/restore         restore previous version → updated metadata
 * DELETE /buckets/{bucket}/objects/{object}/versions        purge all previous versions → 204
 * </pre>
 *
 * <h3>HEAD /buckets/{bucket}/objects/{object} response headers</h3>
 * 
 * <pre>
 * Content-Type, Content-Length, ETag, Last-Modified
 * X-Odilon-Bucket-Name, X-Odilon-Object-Name
 * X-Odilon-Version, X-Odilon-Encrypted, X-Odilon-Raid
 * </pre>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Tag(name = "Resource API", description = "REST-style resource-oriented API. Recommended for all new integrations. " + "See the Legacy API group for the action-based endpoints used by the Java SDK.")
@SecurityRequirement(name = "basicAuth")
@RestController
@RequestMapping(value = "/buckets")
public class ObjectResourceController extends BaseApiController {

	static private Logger logger = Logger.getLogger(ObjectResourceController.class.getName());

	/** X-Odilon-* semantic headers exposed on GET and HEAD object responses */
	private static final String HDR_BUCKET = "X-Odilon-Bucket-Name";
	private static final String HDR_OBJECT = "X-Odilon-Object-Name";
	private static final String HDR_VERSION = "X-Odilon-Version";
	private static final String HDR_ENCRYPT = "X-Odilon-Encrypted";
	private static final String HDR_RAID = "X-Odilon-Raid";

	@JsonIgnore
	@Autowired
	private final ServerSettings settings;

	@Autowired
	public ObjectResourceController(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService, SystemMonitorService monitoringService, TrafficControlService trafficControlService,
			ServerSettings settings) {
		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
		this.settings = settings;
	}

	// =========================================================================
	// BUCKET resources
	// =========================================================================

	/**
	 * GET /buckets Lists all buckets in JSON. Equivalent to GET /bucket/list.
	 */
	@Operation(summary = "List all buckets", description = "Returns a JSON array of all accessible buckets. Equivalent to GET /bucket/list.")
	@ApiResponses({ @ApiResponse(responseCode = "200", description = "Bucket list returned", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Bucket.class))),
			@ApiResponse(responseCode = "401", description = "Unauthorized — invalid credentials", content = @Content), @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<List<Bucket>> listBuckets() {
		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());
			List<Bucket> list = new ArrayList<>();
			getObjectStorageService().findAllBuckets().forEach(b -> list.add(new Bucket(b.getName(), b.getId(), b.getCreationDate(), b.getLastModifiedDate(), b.getStatus())));
			mark();
			return ResponseEntity.ok(list);
		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	/**
	 * PUT /buckets/{bucket} Creates a bucket. Idempotent: returns 201 Created for
	 * new, 200 OK if already exists. Equivalent to PUT /bucket/create/{name}.
	 */
	@Operation(summary = "Create a bucket", description = "Creates a new bucket. Idempotent: returns 200 OK if the bucket already exists, 201 Created otherwise. " + "Equivalent to PUT /bucket/create/{name}.")
	@ApiResponses({ @ApiResponse(responseCode = "201", description = "Bucket created", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Bucket.class))),
			@ApiResponse(responseCode = "200", description = "Bucket already existed — returned as-is", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Bucket.class))),
			@ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content), @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@PutMapping(value = "/{bucket}", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Bucket> createBucket(@Parameter(description = "Bucket name. Must be unique, lowercase, no spaces.", required = true, example = "my-photos") @PathVariable("bucket") String bucketName) {
		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());
			if (getObjectStorageService().existsBucket(bucketName)) {
				io.odilon.virtualFileSystem.model.ServerBucket b = getObjectStorageService().findBucketName(bucketName);
				mark();
				return ResponseEntity.ok(new Bucket(b.getName(), b.getId(), b.getCreationDate(), b.getLastModifiedDate(), b.getStatus()));
			}
			io.odilon.virtualFileSystem.model.ServerBucket created = getObjectStorageService().createBucket(bucketName);
			mark();
			return ResponseEntity.status(HttpStatus.CREATED).body(new Bucket(created.getName(), created.getId(), created.getCreationDate(), created.getLastModifiedDate(), created.getStatus()));
		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	/**
	 * DELETE /buckets/{bucket} Deletes an empty bucket. Returns 204 No Content.
	 * Equivalent to DELETE /bucket/delete/{name}.
	 */
	@Operation(summary = "Delete a bucket", description = "Deletes an empty bucket. The bucket must contain no objects. " + "Equivalent to DELETE /bucket/delete/{name}.")
	@ApiResponses({ @ApiResponse(responseCode = "204", description = "Bucket deleted", content = @Content), @ApiResponse(responseCode = "404", description = "Bucket not found", content = @Content),
			@ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content), @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@DeleteMapping(value = "/{bucket}")
	public ResponseEntity<Void> deleteBucket(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName) {
		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());
			if (!getObjectStorageService().existsBucket(bucketName))
				throw new OdilonObjectNotFoundException("bucket not found -> " + bucketName);
			getObjectStorageService().deleteBucketByName(bucketName);
			mark();
			return ResponseEntity.noContent().build();
		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	/**
	 * HEAD /buckets/{bucket} Returns 200 if the bucket exists, 404 otherwise. No
	 * body.
	 */
	@Operation(summary = "Check bucket exists", description = "Returns 200 OK if the bucket exists, 404 Not Found otherwise. No response body.")
	@ApiResponses({ @ApiResponse(responseCode = "200", description = "Bucket exists", content = @Content), @ApiResponse(responseCode = "404", description = "Bucket not found", content = @Content),
			@ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content) })
	@RequestMapping(value = "/{bucket}", method = RequestMethod.HEAD)
	public ResponseEntity<Void> headBucket(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName) {
		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());
			mark();
			return getObjectStorageService().existsBucket(bucketName) ? ResponseEntity.ok().<Void>build() : ResponseEntity.notFound().build();
		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonInternalErrorException(getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	// =========================================================================
	// OBJECT LIST resource
	// =========================================================================

	/**
	 * GET /buckets/{bucket}/objects Paged object listing. All query params are
	 * optional.
	 *
	 * @param offset   zero-based start position
	 * @param pageSize max items per page
	 * @param prefix   object-name prefix filter
	 * @param agentId  pagination agent id for multi-page queries
	 */
	@Operation(summary = "List objects in a bucket", description = "Returns a paged list of object metadata. Pass the returned `agentId` " + "as the `agentId` query parameter on subsequent requests to iterate through pages.")
	@ApiResponses({ @ApiResponse(responseCode = "200", description = "Object list returned", content = @Content(mediaType = "application/json")), @ApiResponse(responseCode = "404", description = "Bucket not found", content = @Content),
			@ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content), @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@GetMapping(value = "/{bucket}/objects", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<DataList<Item<ObjectMetadata>>> listObjects(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Zero-based start offset", example = "0") @RequestParam("offset") Optional<Long> offset,
			@Parameter(description = "Maximum number of items per page", example = "100") @RequestParam("pageSize") Optional<Long> pageSize,
			@Parameter(description = "Filter objects whose name starts with this prefix", example = "2024/") @RequestParam("prefix") Optional<String> prefix,
			@Parameter(description = "Pagination agent ID returned by the previous page response", example = "") @RequestParam("agentId") Optional<String> agentId) {
		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());
			if (!getObjectStorageService().existsBucket(bucketName))
				throw new OdilonObjectNotFoundException("bucket not found -> " + bucketName);
			mark();
			return ResponseEntity.ok(getObjectStorageService().listObjects(bucketName, offset, pageSize, prefix, agentId));
		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	// =========================================================================
	// OBJECT resources
	// =========================================================================

	/**
	 * GET /buckets/{bucket}/objects/{object} Downloads the object binary with
	 * proper Content-Type, caching and security headers. Equivalent to GET
	 * /object/get/{bucketName}/{objectName}.
	 */
	@Operation(summary = "Download an object", description = "Returns the raw binary of the object with the appropriate Content-Type, " + "ETag, Cache-Control and X-Odilon-* headers. "
			+ "Equivalent to GET /object/get/{bucketName}/{objectName}.")
	@ApiResponses({ @ApiResponse(responseCode = "200", description = "Object binary stream", content = @Content(mediaType = "application/octet-stream")),
			@ApiResponse(responseCode = "404", description = "Object or bucket not found", content = @Content), @ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content),
			@ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@GetMapping(value = "/{bucket}/objects/{object}")
	@ResponseBody
	public ResponseEntity<InputStreamResource> getObject(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Object name", required = true, example = "profile.jpg") @PathVariable("object") String objectName) {

		TrafficPass pass = null;
		InputStream in = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (!getObjectStorageService().existsObject(bucketName, objectName))
				throw new OdilonObjectNotFoundException(notFoundMsg(bucketName, objectName));

			ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);
			if (meta == null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT)
				throw new OdilonObjectNotFoundException(notFoundMsg(bucketName, objectName));

			// Normalise SVG content-type
			if (meta.getFileName() != null && meta.getFileName().toLowerCase().endsWith(".svg"))
				meta.setContentType("image/svg+xml");

			MediaType contentType = resolveContentType(meta);
			long fileLength = getSrcFileLength(meta);
			int cacheSecs = settings.getserverObjectstreamCacheSecs();

			HttpHeaders headers = buildObjectHeaders(meta, fileLength, cacheSecs);

			in = getObjectStorageService().getObjectStream(bucketName, objectName);
			getSystemMonitorService().getGetObjectMeter().mark();
			mark();

			if (fileLength > 0) {
				return ResponseEntity.ok().headers(headers).cacheControl(CacheControl.maxAge(cacheSecs, TimeUnit.SECONDS).cachePublic().immutable()).contentType(contentType).contentLength(fileLength).body(new InputStreamResource(in));
			}
			return ResponseEntity.ok().headers(headers).cacheControl(CacheControl.maxAge(cacheSecs, TimeUnit.SECONDS).cachePublic().immutable()).contentType(contentType).body(new InputStreamResource(in));

		} catch (OdilonServerAPIException e) {
			closeQuietly(in);
			throw e;
		} catch (Exception e) {
			closeQuietly(in);
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	/**
	 * PUT /buckets/{bucket}/objects/{object} Creates or replaces an object
	 * (upsert). Returns 201 Created for new objects, 200 OK for updates. Equivalent
	 * to POST /object/upload/{bucketName}/{objectName}.
	 *
	 * <p>
	 * Required params: {@code file} (multipart), {@code Content-Type}
	 * </p>
	 * <p>
	 * Optional params: {@code fileName}, {@code publicAccess}, {@code customTags}
	 * (pipe-separated, e.g. {@code "tag1||tag2"})
	 * </p>
	 */
	@Operation(summary = "Create or replace an object", description = "Uploads a binary object (upsert). Returns 201 Created for new objects, " + "200 OK when replacing an existing one. "
			+ "Equivalent to POST /object/upload/{bucketName}/{objectName}.")
	@ApiResponses({ @ApiResponse(responseCode = "201", description = "Object created", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ObjectMetadata.class))),
			@ApiResponse(responseCode = "200", description = "Object replaced", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ObjectMetadata.class))),
			@ApiResponse(responseCode = "404", description = "Bucket not found", content = @Content), @ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content),
			@ApiResponse(responseCode = "405", description = "Operation not allowed in current storage mode", content = @Content), @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@PutMapping(value = "/{bucket}/objects/{object}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<ObjectMetadata> putObject(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Object name (key)", required = true, example = "profile.jpg") @PathVariable("object") String objectName,
			@Parameter(description = "File binary content", required = true) @RequestParam("file") MultipartFile file,
			@Parameter(description = "Original file name (defaults to objectName if omitted)", example = "profile.jpg") @RequestParam("fileName") Optional<String> oFileName,
			@Parameter(description = "MIME content type", required = true, example = "image/jpeg") @RequestParam("Content-Type") String contentType,
			@Parameter(description = "Custom tags, pipe-separated (e.g. \"tag1||tag2\")", example = "portrait||2024") @RequestParam("customTags") Optional<String> customTags,
			@Parameter(description = "Mark the object as publicly accessible", example = "false") @RequestParam("publicAccess") Optional<Boolean> oPublic) {

		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			boolean isNew = !getObjectStorageService().existsObject(bucketName, objectName);
			String fileName = oFileName.filter(s -> !s.isEmpty()).orElse(objectName);

			getObjectStorageService().putObject(bucketName, objectName, file.getInputStream(), fileName, contentType, parseTags(customTags), oPublic);

			ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);

			getSystemMonitorService().getPutObjectMeter().mark();
			mark();

			return ResponseEntity.status(isNew ? HttpStatus.CREATED : HttpStatus.OK).contentType(MediaType.APPLICATION_JSON).body(meta);

		} catch (IllegalStateException e) {
			logger.error(e);
			throw new OdilonServerAPIException(ODHttpStatus.METHOD_NOT_ALLOWED, ErrorCode.DATA_STORAGE_MODE_OPERATION_NOT_ALLOWED, getMessage(e));
		} catch (OdilonServerAPIException e) {
			 
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	/**
	 * DELETE /buckets/{bucket}/objects/{object} Deletes an object. Returns 204 No
	 * Content on success, 404 if not found. Equivalent to DELETE
	 * /object/delete/{bucketName}/{objectName}.
	 */
	@Operation(summary = "Delete an object", description = "Deletes an object. Returns 204 No Content on success, 404 if not found. " + "Equivalent to DELETE /object/delete/{bucketName}/{objectName}.")
	@ApiResponses({ @ApiResponse(responseCode = "204", description = "Object deleted", content = @Content), @ApiResponse(responseCode = "404", description = "Object or bucket not found", content = @Content),
			@ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content), @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@DeleteMapping(value = "/{bucket}/objects/{object}")
	public ResponseEntity<Void> deleteObject(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Object name", required = true, example = "profile.jpg") @PathVariable("object") String objectName) {

		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());
			if (!getObjectStorageService().existsObject(bucketName, objectName))
				throw new OdilonObjectNotFoundException(notFoundMsg(bucketName, objectName));
			getObjectStorageService().deleteObject(bucketName, objectName);
			getSystemMonitorService().getDeleteObjectCounter().inc();
			mark();
			return ResponseEntity.noContent().build();
		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	/**
	 * HEAD /buckets/{bucket}/objects/{object} Returns object metadata as HTTP
	 * response headers with no body. This is new functionality with no equivalent
	 * in the legacy action-based API.
	 *
	 * <p>
	 * HTTP 200 response headers:
	 * </p>
	 * 
	 * <pre>
	 * Content-Type       MIME type of the object
	 * Content-Length     source file size in bytes (if known)
	 * ETag               quoted file length
	 * Last-Modified      last modification timestamp
	 * X-Odilon-Bucket-Name
	 * X-Odilon-Object-Name
	 * X-Odilon-Version   current head version number
	 * X-Odilon-Encrypted whether the object is stored encrypted
	 * X-Odilon-Raid      RAID level (raid_0 / raid_1 / raid_6)
	 * </pre>
	 *
	 * <p>
	 * HTTP 404 if the object does not exist, is deleted or is a draft.
	 * </p>
	 */
	@Operation(summary = "Get object metadata as headers", description = "Returns object metadata as HTTP response headers with no body. " + "Useful for checking existence, content-type and size without downloading the payload.")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Object exists — metadata returned in headers", headers = { @Header(name = "Content-Type", description = "MIME type of the object", schema = @Schema(type = "string")),
					@Header(name = "Content-Length", description = "Source file size in bytes", schema = @Schema(type = "integer")), @Header(name = "ETag", description = "Quoted file length", schema = @Schema(type = "string")),
					@Header(name = "Last-Modified", description = "Last modification timestamp", schema = @Schema(type = "string")), @Header(name = "X-Odilon-Bucket-Name", description = "Bucket name", schema = @Schema(type = "string")),
					@Header(name = "X-Odilon-Object-Name", description = "Object name", schema = @Schema(type = "string")), @Header(name = "X-Odilon-Version", description = "Current head version number", schema = @Schema(type = "integer")),
					@Header(name = "X-Odilon-Encrypted", description = "Whether the object is encrypted", schema = @Schema(type = "boolean")),
					@Header(name = "X-Odilon-Raid", description = "RAID level (raid_0/raid_1/raid_6)", schema = @Schema(type = "string")) }, content = @Content),
			@ApiResponse(responseCode = "404", description = "Object not found, deleted or draft", content = @Content), @ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content),
			@ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@RequestMapping(value = "/{bucket}/objects/{object}", method = RequestMethod.HEAD)
	public ResponseEntity<Void> headObject(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Object name", required = true, example = "profile.jpg") @PathVariable("object") String objectName) {

		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (!getObjectStorageService().existsObject(bucketName, objectName)) {
				mark();
				return ResponseEntity.notFound().build();
			}

			ObjectMetadata meta = getObjectStorageService().getObjectMetadata(bucketName, objectName);
			if (meta == null || meta.status == ObjectStatus.DELETED || meta.status == ObjectStatus.DRAFT) {
				mark();
				return ResponseEntity.notFound().build();
			}

			long fileLength = getSrcFileLength(meta);
			int cacheSecs = settings.getserverObjectstreamCacheSecs();

			HttpHeaders headers = buildObjectHeaders(meta, fileLength, cacheSecs);

			// Odilon-specific semantic headers
			headers.set(HDR_BUCKET, bucketName);
			headers.set(HDR_OBJECT, objectName);
			headers.set(HDR_VERSION, String.valueOf(meta.getVersion()));
			headers.set(HDR_ENCRYPT, String.valueOf(meta.isEncrypt()));
			headers.set(HDR_RAID, meta.getRaid() != null ? meta.getRaid() : "");

			getSystemMonitorService().getGetObjectMeter().mark();
			mark();

			return ResponseEntity.ok().headers(headers).build();

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	// =========================================================================
	// PATCH — metadata-only update
	// =========================================================================

	/**
	 * PATCH /buckets/{bucket}/objects/{object} Updates mutable metadata fields
	 * (fileName, contentType, customTags) for an existing object without
	 * re-uploading the binary. The request body must be a JSON representation of
	 * the full {@link ObjectMetadata} document returned by a prior GET or HEAD
	 * call, with the desired fields changed.
	 *
	 * <p>
	 * Returns 200 OK with the updated metadata on success.
	 * </p>
	 */
	@Operation(summary = "Update object metadata", description = "Updates mutable metadata (fileName, contentType, customTags) without " + "re-uploading the object binary. Send the full ObjectMetadata JSON body "
			+ "(as returned by GET /buckets/{bucket}/objects/{object} metadata endpoints) " + "with the desired fields changed.")
	@ApiResponses({ @ApiResponse(responseCode = "200", description = "Metadata updated", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ObjectMetadata.class))),
			@ApiResponse(responseCode = "404", description = "Object or bucket not found", content = @Content), @ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content),
			@ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@PatchMapping(value = "/{bucket}/objects/{object}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<ObjectMetadata> patchObjectMetadata(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Object name", required = true, example = "profile.jpg") @PathVariable("object") String objectName, @RequestBody ObjectMetadata patch) {

		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (!getObjectStorageService().existsObject(bucketName, objectName))
				throw new OdilonObjectNotFoundException(notFoundMsg(bucketName, objectName));

			// Guard: do not allow callers to redirect the patch to a different object
			patch.setBucketName(bucketName);
			patch.setObjectName(objectName);

			ObjectMetadata updated = getObjectStorageService().updateObjectMetadata(patch);
			mark();
			return ResponseEntity.ok(updated);

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	// =========================================================================
	// VERSION sub-resources
	// =========================================================================

	/**
	 * GET /buckets/{bucket}/objects/{object}/versions Returns a JSON array of
	 * {@link ObjectMetadata} for every previous (non-head) version of the object,
	 * in ascending version order. Returns 200 with an empty array if versioning is
	 * disabled or the object has no previous versions.
	 */
	@Operation(summary = "List all previous versions of an object", description = "Returns metadata for every stored previous version of the object " + "(oldest first). Returns an empty array when no previous versions exist.")
	@ApiResponses({ @ApiResponse(responseCode = "200", description = "Version list returned", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ObjectMetadata.class))),
			@ApiResponse(responseCode = "404", description = "Object or bucket not found", content = @Content), @ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content),
			@ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@GetMapping(value = "/{bucket}/objects/{object}/versions", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<List<ObjectMetadata>> listObjectVersions(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Object name", required = true, example = "profile.jpg") @PathVariable("object") String objectName) {

		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (!getObjectStorageService().existsObject(bucketName, objectName))
				throw new OdilonObjectNotFoundException(notFoundMsg(bucketName, objectName));

			List<ObjectMetadata> versions = getObjectStorageService().getObjectMetadataAllPreviousVersions(bucketName, objectName);
			mark();
			return ResponseEntity.ok(versions != null ? versions : new ArrayList<>());

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	/**
	 * GET /buckets/{bucket}/objects/{object}/versions/{version} Downloads the
	 * binary of a specific previous version of an object. Returns 404 if the
	 * version does not exist.
	 */
	@Operation(summary = "Download a specific previous version", description = "Returns the raw binary of the requested version number. " + "Use GET /buckets/{bucket}/objects/{object}/versions to discover available versions.")
	@ApiResponses({ @ApiResponse(responseCode = "200", description = "Version binary stream", content = @Content(mediaType = "application/octet-stream")),
			@ApiResponse(responseCode = "404", description = "Object, bucket or version not found", content = @Content), @ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content),
			@ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@GetMapping(value = "/{bucket}/objects/{object}/versions/{version}")
	@ResponseBody
	public ResponseEntity<InputStreamResource> getObjectVersion(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Object name", required = true, example = "profile.jpg") @PathVariable("object") String objectName,
			@Parameter(description = "Version number (integer)", required = true, example = "2") @PathVariable("version") int version) {

		TrafficPass pass = null;
		InputStream in = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (!getObjectStorageService().existsObject(bucketName, objectName))
				throw new OdilonObjectNotFoundException(notFoundMsg(bucketName, objectName));

			ObjectMetadata meta = getObjectStorageService().getObjectMetadataPreviousVersion(bucketName, objectName, version);
			if (meta == null)
				throw new OdilonObjectNotFoundException(String.format("version %d not found -> b: %s | o: %s", version, bucketName, objectName));

			in = getObjectStorageService().getObjectPreviousVersionStream(bucketName, objectName, version);

			MediaType contentType = resolveContentType(meta);
			long fileLength = getSrcFileLength(meta);
			int cacheSecs = settings.getserverObjectstreamCacheSecs();

			HttpHeaders headers = buildObjectHeaders(meta, fileLength, cacheSecs);
			headers.set(HDR_BUCKET, bucketName);
			headers.set(HDR_OBJECT, objectName);
			headers.set(HDR_VERSION, String.valueOf(version));

			getSystemMonitorService().getGetObjectMeter().mark();
			mark();

			if (fileLength > 0) {
				return ResponseEntity.ok().headers(headers).contentType(contentType).contentLength(fileLength).body(new InputStreamResource(in));
			}
			return ResponseEntity.ok().headers(headers).contentType(contentType).body(new InputStreamResource(in));

		} catch (OdilonServerAPIException e) {
			closeQuietly(in);
			throw e;
		} catch (Exception e) {
			closeQuietly(in);
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	/**
	 * POST /buckets/{bucket}/objects/{object}/restore Restores the most-recent
	 * previous version as the new head version. Returns 200 OK with the updated
	 * {@link ObjectMetadata}. Returns 409 Conflict if the object has no previous
	 * versions to restore.
	 */
	@Operation(summary = "Restore previous version", description = "Promotes the most-recent previous version to become the new head version. " + "Returns 409 Conflict when no previous versions are available.")
	@ApiResponses({ @ApiResponse(responseCode = "200", description = "Version restored — new head metadata returned", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ObjectMetadata.class))),
			@ApiResponse(responseCode = "404", description = "Object or bucket not found", content = @Content), @ApiResponse(responseCode = "409", description = "No previous version to restore", content = @Content),
			@ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content), @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@PostMapping(value = "/{bucket}/objects/{object}/restore", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<ObjectMetadata> restoreObjectVersion(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Object name", required = true, example = "profile.jpg") @PathVariable("object") String objectName) {

		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (!getObjectStorageService().existsObject(bucketName, objectName))
				throw new OdilonObjectNotFoundException(notFoundMsg(bucketName, objectName));

			if (!getObjectStorageService().hasVersions(bucketName, objectName))
				return ResponseEntity.status(HttpStatus.CONFLICT).contentType(MediaType.APPLICATION_JSON).build();

			ObjectMetadata restored = getObjectStorageService().restorePreviousVersion(bucketName, objectName);
			mark();
			return ResponseEntity.ok(restored);

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	/**
	 * DELETE /buckets/{bucket}/objects/{object}/versions Purges all previous
	 * (non-head) versions of the object. The current head version is unaffected.
	 * Returns 204 No Content on success.
	 */
	@Operation(summary = "Purge all previous versions", description = "Permanently deletes every stored previous version of the object. " + "The current head version is unaffected. Returns 204 No Content.")
	@ApiResponses({ @ApiResponse(responseCode = "204", description = "All previous versions purged", content = @Content), @ApiResponse(responseCode = "404", description = "Object or bucket not found", content = @Content),
			@ApiResponse(responseCode = "401", description = "Unauthorized", content = @Content), @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content) })
	@DeleteMapping(value = "/{bucket}/objects/{object}/versions")
	public ResponseEntity<Void> deleteObjectVersions(@Parameter(description = "Bucket name", required = true, example = "my-photos") @PathVariable("bucket") String bucketName,
			@Parameter(description = "Object name", required = true, example = "profile.jpg") @PathVariable("object") String objectName) {

		TrafficPass pass = null;
		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (!getObjectStorageService().existsObject(bucketName, objectName))
				throw new OdilonObjectNotFoundException(notFoundMsg(bucketName, objectName));

			getObjectStorageService().deleteObjectAllPreviousVersions(bucketName, objectName);
			mark();
			return ResponseEntity.noContent().build();

		} catch (OdilonServerAPIException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new OdilonServerAPIException(ODHttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.INTERNAL_ERROR, getMessage(e));
		} finally {
			getTrafficControlService().release(pass);
		}
	}

	@PostConstruct
	public void init() {
	}

	// =========================================================================
	// Private helpers
	// =========================================================================

	/**
	 * Builds the common response headers shared by GET and HEAD object endpoints.
	 * Sets Content-Disposition, Accept-Ranges, Cache-Control, ETag, Last-Modified,
	 * X-Content-Type-Options and CORS exposure headers.
	 */
	private HttpHeaders buildObjectHeaders(ObjectMetadata meta, long fileLength, int cacheSecs) {
		HttpHeaders headers = new HttpHeaders();

		String safeName = meta.getFileName() != null ? meta.getFileName().replace("[", "").replace("]", "") : meta.getObjectName();
		headers.set("Content-Disposition", "inline; filename=\"" + safeName + "\"");
		headers.set(HttpHeaders.ACCEPT_RANGES, "bytes");

		String cacheHeader = cacheSecs > 0 ? "public, max-age=" + cacheSecs + ", immutable, no-transform" : "no-cache, no-store, must-revalidate";
		headers.set(HttpHeaders.CACHE_CONTROL, cacheHeader);

		if (fileLength > 0) {
			headers.setETag("\"" + fileLength + "\"");
			headers.setContentLength(fileLength);
		}

		OffsetDateTime lm = meta.getLastModified();
		if (lm != null)
			headers.setLastModified(lm.toInstant().toEpochMilli());

		headers.set("X-Content-Type-Options", "nosniff");
		headers.set("Access-Control-Allow-Origin", "*");
		headers.set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges, " + HDR_BUCKET + ", " + HDR_OBJECT + ", " + HDR_VERSION + ", " + HDR_ENCRYPT + ", " + HDR_RAID);
		return headers;
	}

	/**
	 * Resolves content type from metadata; falls back to extension-based
	 * estimation.
	 */
	private MediaType resolveContentType(ObjectMetadata meta) {
		if (meta.getContentType() == null || meta.getContentType().equals("application/octet-stream"))
			return estimateContentType(meta.getFileName());
		return MediaType.valueOf(meta.getContentType());
	}

	/**
	 * Parses a "||"-separated tag string into a list. Returns empty Optional if no
	 * tags were supplied.
	 */
	private Optional<List<String>> parseTags(Optional<String> raw) {
		if (raw.isEmpty())
			return Optional.empty();
		List<String> tags = new ArrayList<>();
		for (String s : raw.get().split("\\|\\|"))
			tags.add(s);
		return Optional.of(tags);
	}

	private static String notFoundMsg(String bucket, String object) {
		return String.format("object not found -> b: %s | o: %s", Optional.ofNullable(bucket).orElse("null"), Optional.ofNullable(object).orElse("null"));
	}

	private void closeQuietly(InputStream in) {
		if (in != null) {
			try {
				in.close();
			} catch (IOException e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
		}
	}
}
