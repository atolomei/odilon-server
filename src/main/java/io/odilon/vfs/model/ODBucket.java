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
package io.odilon.vfs.model;

import java.time.OffsetDateTime;

import io.odilon.model.BucketMetadata;
import io.odilon.model.BucketStatus;

/**
 *<p>Odilon stores objects using a flat structure of containers called Buckets.
 * A bucket is like a folder, it just contains binary objects, potentially a very large number. 
 *  Every object contained by a bucket has a unique ObjectName in that bucket; therefore, 
 *  the pair <b>BucketName</b> + <b>ObjectName</b> is a Unique ID for each object in Odilon.</p>
 *  
 *@see {@link Bucket} JSON representation of a Bucket, used both by the server and SDK client
 *@see {@link OdilonBucket} implementation of this interface
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface ODBucket {
	
	public String getName();
	public Long getId();
	public OffsetDateTime getCreationDate();
	public BucketStatus getStatus();
	public BucketMetadata getBucketMetadata();
	public boolean isAccesible();
	public OffsetDateTime getLastModifiedDate();
	
}
