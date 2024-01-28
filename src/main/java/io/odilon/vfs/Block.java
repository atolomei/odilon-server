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

import io.odilon.vfs.model.VFSBucket;

/**
 * 
 * A Block is a chunk of an Object stored in a Drive
 * 
 *  
 * Object -> converted into N blocks (including redundancy)
 * 
 *
 */
public class Block {
			
	private String objectName;
	private VFSBucket bucket;
	
	public Block(VFSBucket bucket, String objectName) {
		this.bucket=bucket;
		this.objectName=objectName;
	}
	
	
	public String getObjectName() {
		return objectName;
	}
	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
	public VFSBucket getBucket() {
		return bucket;
	}
	public void setBucket(VFSBucket bucket) {
		this.bucket = bucket;
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(this.getClass().getSimpleName() +"{");
		str.append("bucketName= '" + (bucket!=null? bucket.getName():"null")+ "' ");
		str.append(", objectName= '" + objectName + "' ");
		str.append("}");
		return str.toString();
	}
	
}
