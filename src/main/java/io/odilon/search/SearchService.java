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

import java.util.List;
import java.util.Map;

import io.odilon.model.ObjectMetadata;

/**
 * <p>
 * Embedded search over bucket / object names and metadata, backed by Apache
 * Lucene. The index is a <b>secondary, rebuildable</b> structure — it is never
 * the source of truth: objects are stored in Odilon storage first, and the
 * index only contains locating and display information.
 * </p>
 * <p>
 * Indexing is asynchronous (see {@link io.odilon.scheduler.SearchServiceRequest})
 * and must never fail a put operation.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface SearchService {

	/** upsert the head version of an object (idempotent) */
	public void index(ObjectMetadata meta);

	/** remove an object from the index */
	public void delete(String bucketName, String objectName);

	/** remove all documents of a bucket (bucket deleted) */
	public void deleteBucket(String bucketName);

	/** bucket renamed -> re-key all documents of the bucket */
	public void renameBucket(String oldBucketName, String newBucketName);

	/**
	 * Search.
	 * 
	 * @param bucketName  optional bucket filter (null -> all buckets)
	 * @param query       free text query over the catch-all field (nullable)
	 * @param metadata    optional exact-match filters on custom metadata keys
	 * @param maxResults  page size
	 * @param offset      0-based offset
	 */
	public List<SearchResult> search(String bucketName, String query, Map<String, String> metadata, int offset, int maxResults);

	/** structured search — all non-null criteria combined with AND semantics */
	public List<SearchResult> search(SearchQuery query);

	/** number of indexed documents matching the query (no document fetching) */
	public long count(SearchQuery query);

	public IndexStatus getIndexStatus();

	public boolean isEnabled();

	/**
	 * map of {@code objectName -> etag} for all documents indexed under a bucket.
	 * Used by the reconciliation job to detect drift and orphans.
	 */
	public Map<String, String> getIndexedEtags(String bucketName);

	/** buckets currently present in the index */
	public List<String> getIndexedBuckets();

	/** mark the index as being rebuilt (reflected in {@link IndexStatus}) */
	public void setRebuilding(boolean value);

	/** durably mark a full rebuild completion in the commit userData */
	public void markRebuildCompleted();

	public void incrementErrorCount();
}
