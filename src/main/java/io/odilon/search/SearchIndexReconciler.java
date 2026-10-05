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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Reconciles the Lucene index with the actual content of the Odilon storage —
 * the storage is always the source of truth.
 * </p>
 * <p>
 * For every bucket it pages through the stored objects and re-indexes any
 * object that is missing from the index or whose {@code etag} has drifted;
 * then removes orphan documents (indexed objects that no longer exist in
 * storage) and documents of buckets that no longer exist. This repairs the
 * drift caused by dropped queue entries, failed index requests, index loss or
 * schema changes.
 * </p>
 * <p>
 * {@code fullRebuild} forces re-indexing of every object regardless of etag,
 * and stamps {@code lastRebuild} in the index commit userData.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class SearchIndexReconciler implements ApplicationContextAware {

	static private Logger logger = Logger.getLogger(SearchIndexReconciler.class.getName());

	static final long PAGE_SIZE = 500;

	@JsonIgnore
	private final SearchService searchService;

	@JsonIgnore
	private volatile ApplicationContext applicationContext;

	@JsonIgnore
	private final AtomicBoolean running = new AtomicBoolean(false);

	public SearchIndexReconciler(SearchService searchService) {
		this.searchService = searchService;
	}

	public boolean isRunning() {
		return this.running.get();
	}

	/**
	 * @param fullRebuild re-index everything regardless of etag
	 * @return true if the reconciliation ran, false if skipped (disabled or
	 *         already running)
	 */
	public boolean reconcile(boolean fullRebuild) {

		if (!getSearchService().isEnabled())
			return false;

		if (!this.running.compareAndSet(false, true)) {
			logger.warn("reconciliation already running -> skipped");
			return false;
		}

		long start = System.currentTimeMillis();
		long indexed = 0;
		long removed = 0;

		getSearchService().setRebuilding(true);

		try {
			Set<String> liveBuckets = new HashSet<String>();

			for (ServerBucket bucket : getVirtualFileSystemService().listAllBuckets()) {

				String bucketName = bucket.getName();
				liveBuckets.add(bucketName);

				Map<String, String> indexedEtags = getSearchService().getIndexedEtags(bucketName);
				Set<String> seen = new HashSet<String>();

				/** walk storage in pages */
				Long offset = Long.valueOf(0);
				String agentId = null;
				boolean done = false;

				while (!done) {
					DataList<Item<ObjectMetadata>> page = getVirtualFileSystemService().listObjects(bucketName, Optional.of(offset), Optional.of(Long.valueOf(PAGE_SIZE)), Optional.empty(), Optional.ofNullable(agentId));
					agentId = page.getAgentId();

					for (Item<ObjectMetadata> item : page.getList()) {
						if (!item.isOk())
							continue;
						ObjectMetadata meta = item.getObject();
						seen.add(meta.objectName);

						String indexedEtag = indexedEtags.get(meta.objectName);
						if (fullRebuild || indexedEtag == null || !indexedEtag.equals(meta.etag != null ? meta.etag : "")) {
							getSearchService().index(meta);
							indexed++;
						}
					}

					offset = Long.valueOf(offset.longValue() + page.getList().size());
					done = page.isEOD() || page.getList().isEmpty();
				}

				/** orphans: indexed but no longer in storage */
				for (String objectName : indexedEtags.keySet()) {
					if (!seen.contains(objectName)) {
						getSearchService().delete(bucketName, objectName);
						removed++;
					}
				}
			}

			/** buckets present in the index but no longer in storage */
			for (String indexedBucket : getSearchService().getIndexedBuckets()) {
				if (!liveBuckets.contains(indexedBucket)) {
					getSearchService().deleteBucket(indexedBucket);
					removed++;
				}
			}

			if (fullRebuild)
				getSearchService().markRebuildCompleted();

			logger.info("Search index reconciliation done" + (fullRebuild ? " (full rebuild)" : "") + " -> indexed: " + indexed + " | removed: " + removed + " | duration: " + (System.currentTimeMillis() - start) + " ms");
			return true;

		} catch (Exception e) {
			getSearchService().incrementErrorCount();
			logger.error(e, SharedConstant.NOT_THROWN);
			return false;

		} finally {
			getSearchService().setRebuilding(false);
			this.running.set(false);
		}
	}

	public SearchService getSearchService() {
		return this.searchService;
	}

	public VirtualFileSystemService getVirtualFileSystemService() {
		return this.applicationContext.getBean(VirtualFileSystemService.class);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
