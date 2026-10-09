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
package io.odilon.encryption;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.error.OdilonServerAPIException;
import io.odilon.log.Logger;
import io.odilon.model.DataStorage;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.util.DateTimeUtil;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Async process that encrypts Objects stored unencrypted (e.g. Objects
 * uploaded before encryption was enabled).
 * </p>
 * <p>
 * It walks every bucket (or a single one) in pages, and for each Object whose
 * {@link ObjectMetadata#isEncrypt()} is {@code false} it re-stores the Object
 * through the regular {@code putObject} update path, so the RAID driver,
 * journal, version control, caches and search index behave exactly as for a
 * client update. With version control enabled the previous version remains
 * unencrypted.
 * </p>
 * <p>
 * The check + update is done under the Object write lock and the metadata is
 * re-read inside the critical section, so Objects created or updated after the
 * process started (which are already encrypted because encryption is enabled)
 * are skipped and never overwritten with stale plaintext. The process is
 * idempotent: it can be re-run at any time.
 * </p>
 * <p>
 * Modelled on {@code RAIDOneDriveSync} and {@code SearchIndexReconciler}.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class ObjectEncryptionProcess implements ApplicationContextAware {

	static private Logger logger = Logger.getLogger(ObjectEncryptionProcess.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	static final long PAGE_SIZE = ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE;
	static final String THREAD_NAME = "object-encryption";

	@JsonIgnore
	private volatile ApplicationContext applicationContext;

	@JsonIgnore
	private final AtomicBoolean running = new AtomicBoolean(false);

	@JsonIgnore
	private volatile Thread thread;

	/** last / current run */
	@JsonIgnore
	private volatile String status = EncryptionProcessStatus.IDLE;
	@JsonIgnore
	private volatile String bucketFilter;
	@JsonIgnore
	private volatile String currentBucket;
	@JsonIgnore
	private volatile OffsetDateTime started;
	@JsonIgnore
	private volatile OffsetDateTime finished;
	@JsonIgnore
	private volatile int threads = 0;
	@JsonIgnore
	private volatile String message;

	@JsonIgnore
	private final AtomicLong scanned = new AtomicLong(0);
	@JsonIgnore
	private final AtomicLong encrypted = new AtomicLong(0);
	@JsonIgnore
	private final AtomicLong skipped = new AtomicLong(0);
	@JsonIgnore
	private final AtomicLong notAvailable = new AtomicLong(0);
	@JsonIgnore
	private final AtomicLong errors = new AtomicLong(0);
	@JsonIgnore
	private final AtomicLong totalBytes = new AtomicLong(0);

	public ObjectEncryptionProcess() {
	}

	public boolean isRunning() {
		return this.running.get();
	}

	/**
	 * <p>
	 * Validates preconditions and starts the process in a background daemon
	 * thread. Returns immediately.
	 * </p>
	 * 
	 * @param bucketName optional, restrict the process to this bucket
	 * 
	 * @throws OdilonServerAPIException if encryption is not enabled / not
	 *                                  initialized, the data storage is not
	 *                                  writable, the bucket does not exist or the
	 *                                  process is already running
	 */
	public synchronized void start(Optional<String> bucketName) {

		VirtualFileSystemService vfs = getVirtualFileSystemService();

		if (!vfs.isEncrypt())
			throw new OdilonServerAPIException("encryption is not enabled (encryption.enabled=false)");

		OdilonServerInfo info = vfs.getOdilonServerInfo();
		if (info == null || !info.isEncryptionIntialized())
			throw new OdilonServerAPIException("encryption is not initialized");

		if (vfs.getEncryptionService() == null)
			throw new OdilonServerAPIException("encryption service is not available");

		DataStorage dataStorage = vfs.getServerSettings().getDataStorage();
		if (dataStorage == DataStorage.READONLY)
			throw new OdilonServerAPIException("Data Storage is in read only mode");
		if (dataStorage == DataStorage.WORM)
			throw new OdilonServerAPIException("Data Storage is in WORM mode, can not update existing objects");

		final String filter = (bucketName.isPresent() && !bucketName.get().isBlank()) ? bucketName.get().trim() : null;

		if (filter != null && !vfs.existsBucket(filter))
			throw new OdilonServerAPIException("bucket does not exist -> " + filter);

		if (!this.running.compareAndSet(false, true))
			throw new OdilonServerAPIException("object encryption process is already running");

		/** reset counters for this run */
		this.bucketFilter = filter;
		this.currentBucket = null;
		this.started = DateTimeUtil.now();
		this.finished = null;
		this.message = null;
		this.status = EncryptionProcessStatus.RUNNING;
		this.scanned.set(0);
		this.encrypted.set(0);
		this.skipped.set(0);
		this.notAvailable.set(0);
		this.errors.set(0);
		this.totalBytes.set(0);

		this.thread = new Thread(() -> {
			try {
				run(filter);
			} catch (Throwable e) {
				this.status = EncryptionProcessStatus.FAILED;
				this.message = e.getClass().getName() + (e.getMessage() != null ? " | " + e.getMessage() : "");
				logger.error(e, SharedConstant.NOT_THROWN);
			} finally {
				this.finished = DateTimeUtil.now();
				this.currentBucket = null;
				this.running.set(false);
			}
		});
		this.thread.setDaemon(true);
		this.thread.setName(THREAD_NAME);
		this.thread.start();
	}

	public EncryptionProcessStatus getStatus() {
		EncryptionProcessStatus s = new EncryptionProcessStatus();
		s.status = this.status;
		s.bucket = this.bucketFilter;
		s.currentBucket = this.currentBucket;
		s.started = this.started;
		s.finished = this.finished;
		s.threads = this.threads;
		s.scanned = this.scanned.get();
		s.encrypted = this.encrypted.get();
		s.skipped = this.skipped.get();
		s.notAvailable = this.notAvailable.get();
		s.errors = this.errors.get();
		s.totalBytes = this.totalBytes.get();
		s.message = this.message;
		if (this.started != null) {
			OffsetDateTime end = (this.finished != null) ? this.finished : DateTimeUtil.now();
			s.durationMillis = java.time.Duration.between(this.started, end).toMillis();
		}
		return s;
	}

	/**
	 * Walks the buckets in pages and re-stores unencrypted Objects.
	 */
	private void run(String filter) {

		long start_ms = System.currentTimeMillis();

		logger.info("Starting -> " + this.getClass().getSimpleName() + (filter != null ? " | bucket: " + filter : " | all buckets"));

		this.threads = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.0).intValue() + 1;

		ExecutorService executor = Executors.newFixedThreadPool(this.threads);

		try {

			List<ServerBucket> buckets = new ArrayList<ServerBucket>();

			if (filter != null)
				buckets.add(getVirtualFileSystemService().getBucketByName(filter));
			else
				buckets.addAll(getVirtualFileSystemService().listAllBuckets());

			for (ServerBucket bucket : buckets) {

				this.currentBucket = bucket.getName();

				Long offset = Long.valueOf(0);
				String agentId = null;
				boolean done = false;

				while (!done) {

					DataList<Item<ObjectMetadata>> page = getVirtualFileSystemService().listObjects(bucket.getName(), Optional.of(offset), Optional.of(Long.valueOf(PAGE_SIZE)), Optional.empty(),
							Optional.ofNullable(agentId));

					if (agentId == null)
						agentId = page.getAgentId();

					List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(page.getList().size());

					for (Item<ObjectMetadata> item : page.getList()) {
						tasks.add(() -> {
							process(bucket, item);
							return null;
						});
					}

					try {
						executor.invokeAll(tasks, 15, TimeUnit.MINUTES);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						logger.error(e, SharedConstant.NOT_THROWN);
						this.status = EncryptionProcessStatus.FAILED;
						this.message = "interrupted";
						return;
					}

					offset = Long.valueOf(offset.longValue() + page.getList().size());
					done = page.isEOD() || page.getList().isEmpty();
				}
			}

			this.status = (this.errors.get() > 0) ? EncryptionProcessStatus.FAILED : EncryptionProcessStatus.COMPLETED;
			if (this.errors.get() > 0)
				this.message = "completed with errors";

		} finally {

			try {
				executor.shutdown();
				executor.awaitTermination(15, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}

			startuplogger.info(ServerConstant.SEPARATOR);
			startuplogger.info(this.getClass().getSimpleName() + " completed" + (filter != null ? " | bucket: " + filter : ""));
			startuplogger.info("Threads: " + String.valueOf(this.threads));
			startuplogger.info("Total objects scanned: " + String.valueOf(this.scanned.get()));
			startuplogger.info("Total objects encrypted: " + String.valueOf(this.encrypted.get()));
			startuplogger.info("Total objects skipped (already encrypted): " + String.valueOf(this.skipped.get()));
			double val = Double.valueOf(this.totalBytes.get()).doubleValue() / SharedConstant.d_gigabyte;
			startuplogger.info("Total size: " + String.format("%14.4f", val).trim() + " GB");

			if (this.errors.get() > 0)
				startuplogger.info("Errors: " + String.valueOf(this.errors.get()));

			if (this.notAvailable.get() > 0)
				startuplogger.info("Not available: " + String.valueOf(this.notAvailable.get()));

			startuplogger.info("Duration: " + String.valueOf(Double.valueOf(System.currentTimeMillis() - start_ms) / Double.valueOf(1000)) + " secs");
			startuplogger.info(ServerConstant.SEPARATOR);
		}
	}

	/**
	 * <p>
	 * Processes one listed Object. Errors are counted and logged, they do not
	 * abort the run.
	 * </p>
	 */
	private void process(ServerBucket bucket, Item<ObjectMetadata> item) {

		try {

			long n = this.scanned.incrementAndGet();
			if ((n % 100) == 0)
				logger.info("scanned so far -> " + String.valueOf(n) + " | encrypted -> " + String.valueOf(this.encrypted.get()));

			if (!item.isOk()) {
				this.notAvailable.incrementAndGet();
				return;
			}

			ObjectMetadata listed = item.getObject();

			/** cheap pre-check on the listing snapshot */
			if (listed.isEncrypt()) {
				this.skipped.incrementAndGet();
				return;
			}

			encryptObject(bucket, listed.getObjectName());

		} catch (Exception e) {
			this.errors.incrementAndGet();
			logger.error(e, "b:" + bucket.getName() + " o:" + (item.getObject() != null ? item.getObject().getObjectName() : "null"), SharedConstant.NOT_THROWN);
		}
	}

	/**
	 * <p>
	 * Re-stores the Object encrypted. Runs under the Object write lock + bucket
	 * read lock (both reentrant, the inner update handler re-acquires them from
	 * the same thread). The metadata is re-read inside the critical section so
	 * concurrent client updates (already encrypted) are never overwritten.
	 * </p>
	 */
	private void encryptObject(ServerBucket bucket, String objectName) throws Exception {

		VirtualFileSystemService vfs = getVirtualFileSystemService();

		getLockService().getObjectLock(bucket, objectName).writeLock().lock();

		try {

			getLockService().getBucketLock(bucket).readLock().lock();

			try {

				if (!vfs.existsObject(bucket, objectName)) {
					this.notAvailable.incrementAndGet();
					return;
				}

				/** authoritative re-read */
				ObjectMetadata meta = vfs.getObjectMetadata(bucket, objectName);

				if (meta == null) {
					this.notAvailable.incrementAndGet();
					return;
				}

				if (meta.isEncrypt()) {
					this.skipped.incrementAndGet();
					return;
				}

				/**
				 * copy plaintext to a temp file first: the update handler rewrites the data
				 * file we would otherwise be reading from
				 */
				File tmp = Files.createTempFile("odilon-enc-", ".tmp").toFile();

				try {

					long bytes = 0;

					try (InputStream in = new BufferedInputStream(vfs.getObjectStream(bucket, objectName), ServerConstant.BUFFER_SIZE);
							BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(tmp), ServerConstant.BUFFER_SIZE)) {
						byte[] buf = new byte[ServerConstant.BUFFER_SIZE];
						int read;
						while ((read = in.read(buf, 0, buf.length)) >= 0) {
							out.write(buf, 0, read);
							bytes += read;
						}
					}

					try (InputStream in = new BufferedInputStream(new FileInputStream(tmp), ServerConstant.BUFFER_SIZE)) {
						vfs.putObject(bucket, objectName, in, meta.getFileName(), meta.getContentType(), Optional.ofNullable(meta.getCustomTags()), Optional.of(Boolean.valueOf(meta.isPublicAccess())));
					}

					this.encrypted.incrementAndGet();
					this.totalBytes.addAndGet(bytes);

					logger.debug("encrypted -> b:" + bucket.getName() + " o:" + objectName + " | " + bytes + " bytes");

				} finally {
					try {
						Files.deleteIfExists(tmp.toPath());
					} catch (Exception e) {
						logger.warn("can not delete temp file -> " + tmp.getAbsolutePath());
					}
				}

			} finally {
				getLockService().getBucketLock(bucket).readLock().unlock();
			}

		} finally {
			getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
		}
	}

	protected LockService getLockService() {
		return getVirtualFileSystemService().getLockService();
	}

	public VirtualFileSystemService getVirtualFileSystemService() {
		return this.applicationContext.getBean(VirtualFileSystemService.class);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
