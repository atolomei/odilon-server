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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;

/**
*  <p>Bucket iterator</p>
*  
* @author atolomei@novamens.com (Alejandro Tolomei)
*/
public abstract class BucketIterator implements Iterator<Path>  {
				
	private static final Logger logger = Logger.getLogger(BucketIterator.class.getName());

	static private ObjectMapper mapper = new ObjectMapper();
	
	static  {
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
	
	@JsonProperty("agentId")
	private String agentId = null;
	
	@JsonProperty("offset")
	private Long offset = Long.valueOf(0);
	
	@JsonIgnore
	private final ServerBucket bucket;

	@JsonProperty("bucketId")
	private final Long bucketId;

	@JsonProperty("bucketName")
	private final String bucketName;
	
	/**
	 * @param bucketName can not be null
	 */
	public BucketIterator(final ServerBucket bucket) {
		Check.requireNonNullArgument(bucket, "bucket is null");
		this.bucket=bucket;
		this.bucketId=bucket.getId();
		this.bucketName=bucket.getName();
	}
	
	public String getAgentId() {
		return agentId;
	}

	public void setAgentId(String agentId) {
		this.agentId = agentId;
	}
	
	
	public ServerBucket getBucket() {
		return this.bucket;
	}
	
	public Long getBucketId() {
		return bucket.getId();
	}

	public Long getOffset() {
		return offset;
	}

	public Long setOffset(Long offset) {
		this.offset = offset;
		return offset;
	}

	public void close() throws IOException  {
	}

	@Override
	public String toString() {
			StringBuilder str = new StringBuilder();
			str.append(this.getClass().getSimpleName() +"{");
			str.append(toJSON());
			str.append("}");
			return str.toString();
	}
		
	 public String toJSON() {
	   try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
					logger.error(e, SharedConstant.NOT_THROWN);
					return "\"error\":\"" + e.getClass().getName()+ " | " + e.getMessage()+"\""; 
		}
	  }
	
	 
	
}
