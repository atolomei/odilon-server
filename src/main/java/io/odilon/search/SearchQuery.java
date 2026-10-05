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
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Structured search parameters for {@link SearchService#search(SearchQuery)}.
 * All fields are optional; non-null fields are combined with AND semantics.
 * </p>
 * <ul>
 * <li>{@code bucketName} — exact bucket filter</li>
 * <li>{@code q} — free text over the catch-all field</li>
 * <li>{@code objectName} — tokenized, case-insensitive match on the object key</li>
 * <li>{@code fileName} — tokenized, case-insensitive match on the file name</li>
 * <li>{@code lastModifiedFrom} / {@code lastModifiedTo} — inclusive date range</li>
 * <li>{@code metadata} — exact-match filters on custom metadata keys</li>
 * </ul>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class SearchQuery implements Serializable {

	private static final long serialVersionUID = 1L;

	public String bucketName;

	/** free text over the catch-all field */
	public String q;

	/** tokenized match on the object key */
	public String objectName;

	/** tokenized match on the file name */
	public String fileName;

	/** inclusive range on lastModified */
	public OffsetDateTime lastModifiedFrom;
	public OffsetDateTime lastModifiedTo;

	/** exact-match filters on custom metadata keys (meta.<key>) */
	public Map<String, String> metadata = new HashMap<String, String>();

	public int offset = 0;

	public int maxResults = 1000;

	public SearchQuery() {
	}

	public boolean isEmpty() {
		return (q == null || q.isBlank()) && (objectName == null || objectName.isBlank()) && (fileName == null || fileName.isBlank()) && lastModifiedFrom == null && lastModifiedTo == null
				&& (metadata == null || metadata.isEmpty()) && (bucketName == null || bucketName.isBlank());
	}
}
