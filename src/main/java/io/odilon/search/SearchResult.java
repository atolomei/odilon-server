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
package io.odilon.search;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>
 * A single search hit. Contains only locating / display information — Lucene is
 * <b>not</b> the source of truth, the object lives in Odilon storage.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class SearchResult implements Serializable {

	private static final long serialVersionUID = 1L;

	@JsonProperty("bucketName")
	public String bucketName;

	@JsonProperty("objectName")
	public String objectName;

	@JsonProperty("fileName")
	public String fileName;

	@JsonProperty("size")
	public long size;

	@JsonProperty("contentType")
	public String contentType;

	@JsonProperty("lastModified")
	public OffsetDateTime lastModified;

	@JsonProperty("etag")
	public String etag;

	@JsonProperty("tags")
	public List<String> tags = new ArrayList<String>();

	@JsonProperty("score")
	public float score;

	public SearchResult() {
	}
}
