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
package io.odilon.vfs;

import java.time.OffsetDateTime;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.model.BucketMetadata;
import io.odilon.model.BucketStatus;
import io.odilon.model.OdilonModelObject;
import io.odilon.vfs.model.DriveBucket;
import io.odilon.vfs.model.ServerBucket;


/**
 *  
 *  <p>Server representation of a Bucket. A bucket is like a folder, it just contains binary objects, potentially a very large number.</p>
 *  
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class OdilonBucket extends OdilonModelObject implements ServerBucket {

	@JsonProperty("bucketMetadata")
	final private BucketMetadata meta;
	
	public OdilonBucket(DriveBucket db) {
		this.meta=db.getBucketMetadata();
	}

	public OdilonBucket(BucketMetadata meta) {
		this.meta=meta;
	}

	@Override
	public boolean isAccesible() {
		return this.meta.status==BucketStatus.ENABLED || meta.status==BucketStatus.ARCHIVED;
	}
	
	@Override
	public String getName() {
		return this.meta.bucketName;
	}
	
	public OffsetDateTime getCreationDate() {
		return meta.creationDate;
	}
	
	public OffsetDateTime getLastModifiedDate() {
		return meta.lastModified;
	}
	
	@Override
	public BucketStatus getStatus() {
		return meta.status;
	}
	
	@Override
	public BucketMetadata getBucketMetadata() {
		return meta;
	}
	
	@Override
	public Long getId() {
		return meta.id;
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(this.getClass().getSimpleName() +"{");
		str.append("\"name\":" + (Optional.ofNullable(meta.bucketName).isPresent() ? ("\""+meta.bucketName+"\"") :"null"));
		str.append("\"id\":" + (Optional.ofNullable(meta.id).isPresent() ? ("+meta.id+") :"null"));
		str.append(", \"status\":" + (Optional.ofNullable(meta.status).isPresent() ? ("\""+meta.status.getName()+"\"") :"null"));
		str.append(", \"creationDate\":" + (meta.creationDate!=null ? ("\""+meta.creationDate+"\"") :"null"));
		str.append("}");
		return str.toString();
	}

}
