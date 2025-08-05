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
package io.odilon.virtualFileSystem.model;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.log.Logger;
import io.odilon.model.BucketMetadata;
import io.odilon.model.BucketStatus;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class DriveBucket {

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(DriveBucket.class.getName());
	static private ObjectMapper objectMapper = new ObjectMapper();

	static {
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	private BucketMetadata meta;

	/** this field may cause infinite recursion if not ignored */

	@JsonIgnore
	private Drive drive;

	public DriveBucket(Drive drive, BucketMetadata vfs_meta) throws IOException {
		this.drive = drive;
		this.meta = vfs_meta.clone();
	}

	//@JsonIgnore
	//public Path getBucketControlDir() {
	//	return Paths.get(getDrive().getBucketMetadataDirPath(meta.id));
	//}

	@JsonIgnore
	public Drive getDrive() {
		return drive;
	}

	public String getName() {
		return meta.bucketName;
	}

	public Long getId() {
		return meta.id;
	}

	public OffsetDateTime getCreationDate() {
		return meta.creationDate;
	}

	public BucketMetadata getBucketMetadata() {
		return meta;
	}

	public BucketStatus getStatus() {
		return meta.status;
	}

	@JsonIgnore
	public boolean isDeleted() {
		if (meta.status == null)
			return false;
		return meta.status == BucketStatus.DELETED;
	}

	@JsonIgnore
	public boolean isAccesible() {
		if (meta.status == null)
			return false;
		return meta.status == BucketStatus.ENABLED || meta.status == BucketStatus.ARCHIVED;
	}

}
