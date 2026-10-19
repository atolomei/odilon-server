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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

	/** commit the Lucene writer every N buffered changes instead of per document */
	static final int COMMIT_BATCH_SIZE = 500;

	@JsonIgnore
	private final SearchService searchService;

	@JsonIgnore
	private volatile ApplicationContext applicationContext;

	@JsonIgnore
	private final AtomicBoolean running = new AtomicBoolean(false);

	/** progress counters — valid while {@link #isRunning()} is true, reset at the start of each run */
	@JsonIgnore
	private final AtomicInteger totalBuckets = new AtomicInteger(0);
	@JsonIgnore
	private final AtomicInteger processedBuckets = new AtomicInteger(0);
	@JsonIgnore
	private final AtomicReference<String> currentBucket = new AtomicReference<String>(null);
	@JsonIgnore
	private final AtomicLong processedObjects = new AtomicLong(0);
	@JsonIgnore
	private final AtomicLong indexedCount = new AtomicLong(0);
	@JsonIgnore
	private final AtomicLong removedCount = new AtomicLong(0);
	@JsonIgnore
	private volatile long startedAt = 0L;

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

		/** reset progress counters for this run */
		this.startedAt = start;
		this.processedBuckets.set(0);
		this.processedObjects.set(0);
		this.indexedCount.set(0);
		this.removedCount.set(0);
		this.currentBucket.set(null);

		getSearchService().setRebuilding(true);

		try {
			Set<String> liveBuckets = new HashSet<String>();
			int uncommitted = 0;

			List<ServerBucket> buckets = getVirtualFileSystemService().listAllBuckets();
			this.totalBuckets.set(buckets.size());

			for (ServerBucket bucket : buckets) {

				String bucketName = bucket.getName();
				liveBuckets.add(bucketName);
				this.currentBucket.set(bucketName);

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
						this.processedObjects.incrementAndGet();
						if (!item.isOk())
							continue;
						ObjectMetadata meta = item.getObject();
						seen.add(meta.objectName);

						String indexedEtag = indexedEtags.get(meta.objectName);
						if (fullRebuild || indexedEtag == null || !indexedEtag.equals(meta.etag != null ? meta.etag : "")) {
							getSearchService().index(meta, false);
							indexed++;
							this.indexedCount.incrementAndGet();
							if (++uncommitted >= COMMIT_BATCH_SIZE) {
								getSearchService().commitPending();
								uncommitted = 0;
							}
						}
					}

					offset = Long.valueOf(offset.longValue() + page.getList().size());
					done = page.isEOD() || page.getList().isEmpty();
				}

				/** orphans: indexed but no longer in storage */
				for (String objectName : indexedEtags.keySet()) {
					if (!seen.contains(objectName)) {
						getSearchService().delete(bucketName, objectName, false);
						removed++;
						this.removedCount.incrementAndGet();
						if (++uncommitted >= COMMIT_BATCH_SIZE) {
							getSearchService().commitPending();
							uncommitted = 0;
						}
					}
				}

				this.processedBuckets.incrementAndGet();
			}

			/** buckets present in the index but no longer in storage */
			for (String indexedBucket : getSearchService().getIndexedBuckets()) {
				if (!liveBuckets.contains(indexedBucket)) {
					getSearchService().deleteBucket(indexedBucket, false);
					removed++;
					this.removedCount.incrementAndGet();
					uncommitted++;
				}
			}

			/** flush any remaining buffered changes before stamping rebuild completion */
			if (uncommitted > 0) {
				getSearchService().commitPending();
				uncommitted = 0;
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
			/** make sure any buffered changes are not left uncommitted, even on error */
			try {
				getSearchService().commitPending();
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
			}
			getSearchService().setRebuilding(false);
			this.currentBucket.set(null);
			this.running.set(false);
		}
	}

	/**
	 * Fills {@code status} with a progress snapshot of the currently running (or
	 * last) reconciliation. Only meaningful while {@link #isRunning()} is true.
	 */
	public void fillProgress(IndexStatus status) {
		status.rebuildRunning = this.running.get();
		status.rebuildTotalBuckets = this.totalBuckets.get();
		status.rebuildProcessedBuckets = this.processedBuckets.get();
		status.rebuildCurrentBucket = this.currentBucket.get();
		status.rebuildProcessedObjects = this.processedObjects.get();
		status.rebuildIndexed = this.indexedCount.get();
		status.rebuildRemoved = this.removedCount.get();
		status.rebuildStartedAt = this.startedAt;
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
