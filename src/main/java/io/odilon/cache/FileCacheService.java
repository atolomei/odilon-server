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
package io.odilon.cache;

import java.io.File;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

import jakarta.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * {@link File} cache used by {@link RAIDSixDriver} (the other RAID
 * configurations do not need it).
 * </p>
 * <p>
 * It Uses {@link Caffeine} to keep references to entries in memory. On eviction
 * it has to remove the {@link File} from the File System
 * ({@link FileCacheService#onRemoval}).
 * </p>
 * <p>
 * Listens to Spring Event {@link CacheEvent} that is fired by the
 * {@link JournalService} on {@code commit} or {@code cancel}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
@Service
public class FileCacheService extends BaseService implements ApplicationListener<CacheEvent> {

	static private Logger logger = Logger.getLogger(FileCacheService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	private AtomicLong cacheSizeBytes = new AtomicLong(0);

	@JsonIgnore
	@Autowired
	private final LockService vfsLockService;

	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	/** lazy injection (otherwise there are circular dependencies) */
	@JsonIgnore
	private VirtualFileSystemService vfs;

	@JsonIgnore
	private List<Drive> listDrives;

	@JsonIgnore
	private Cache<String, File> cache;

	/**
	 * <p>
	 * This File cache uses a {@link Caffeine} based cache of references in memory
	 * </p>
	 * 
	 */
	public FileCacheService(ServerSettings serverSettings, LockService vfsLockService) {
		this.serverSettings = serverSettings;
		this.vfsLockService = vfsLockService;
	}

	public boolean containsKey(Long bucketId, String objectName, Optional<Integer> version) {

		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketId.toString());

		getLockService().getFileCacheLock(bucketId, objectName, version).readLock().lock();

		try {
			return (getCache().getIfPresent(getKey(bucketId, objectName, version)) != null);
		} finally {
			getLockService().getFileCacheLock(bucketId, objectName, version).readLock().unlock();
		}
	}

	/**
	 * @param bucketName
	 * @param objectName
	 * @return File from cache, null if it is not present
	 * 
	 */
	public File get(Long bucketId, String objectName, Optional<Integer> version) {

		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketId.toString());

		getLockService().getFileCacheLock(bucketId, objectName, version).readLock().lock();
		try {
			return getCache().getIfPresent(getKey(bucketId, objectName, version));
		} finally {
			getLockService().getFileCacheLock(bucketId, objectName, version).readLock().unlock();
		}
	}

	/**
	 * <p>
	 * If the lock was set by the calling object, we do not apply it here
	 * </p>
	 * 
	 * @param bucketName
	 * @param objectName
	 * @param file
	 */
	public void put(Long bucketId, String objectName, Optional<Integer> version, File file, boolean lockRequired) {

		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketId.toString());
		Check.requireNonNullArgument(file, "file is null");

		if (lockRequired)
			getLockService().getFileCacheLock(bucketId, objectName, version).writeLock().lock();
		try {
			getCache().put(getKey(bucketId, objectName, version), file);
			this.cacheSizeBytes.getAndAdd(file.length());
		} finally {
			if (lockRequired)
				getLockService().getFileCacheLock(bucketId, objectName, version).writeLock().unlock();
		}
	}

	/**
	 * @param bucketName
	 * @param objectName
	 */
	public void remove(Long bucketId, String objectName, Optional<Integer> version) {

		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketId.toString());

		getLockService().getFileCacheLock(bucketId, objectName, version).writeLock().lock();

		try {

			File file = getCache().getIfPresent(getKey(bucketId, objectName, version));
			getCache().invalidate(getKey(bucketId, objectName, version));

			if (file != null) {
				FileUtils.deleteQuietly(file);
				this.cacheSizeBytes.getAndAdd(-file.length());
			}
		} finally {
			getLockService().getFileCacheLock(bucketId, objectName, version).writeLock().unlock();
		}
	}

	/**
	 * @param bucketName
	 * @param objectName
	 * @return String with the absolute path where to save the File cache
	 */
	public String getFileCachePath(Long bucketId, String objectName, Optional<Integer> version) {
		String path = getKey(bucketId, objectName, version);
		return getDrivesAll().get(Math.abs(path.hashCode()) % getDrivesAll().size()).getCacheDirPath() + File.separator
				+ path;
	}

	/**
	 * @return the approximate number of entries in this cache
	 */
	public long size() {
		return getCache().estimatedSize();
	}

	/**
	 * @return the hard disk usage in bytes
	 */
	public long hardDiskUsage() {
		return this.cacheSizeBytes.get();
	}

	public synchronized void setVirtualFileSystemService(VirtualFileSystemService virtualFileSystemService) {
		try {
			this.vfs = virtualFileSystemService;
		} finally {
			setStatus(ServiceStatus.RUNNING);
			startuplogger.debug("Started -> " + FileCacheService.class.getSimpleName());
		}
	}

	public VirtualFileSystemService getVirtualFileSystemService() {
		if (this.vfs == null) {
			logger.error("The instance of " + VirtualFileSystemService.class.getSimpleName()
					+ " must be setted during the @PostConstruct method of the " + this.getClass().getName()
					+ " instance. It can not be injected via AutoWired beacause of circular dependencies.");
			throw new IllegalStateException(VirtualFileSystemService.class.getSimpleName()
					+ " instance is null. it must be setted during the @PostConstruct method of the "
					+ this.getClass().getName() + " instance");
		}
		return this.vfs;
	}

	public LockService getLockService() {
		return this.vfsLockService;
	}

	/**
	 * 
	 */
	@Override
	public void onApplicationEvent(CacheEvent event) {

		if (event.getOperation().getOperationCode() == OperationCode.CREATE_OBJECT) {
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(), Optional.empty());
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(),
					Optional.of(event.getOperation().getVersion()));
			return;
		}
		if (event.getOperation().getOperationCode() == OperationCode.UPDATE_OBJECT) {
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(), Optional.empty());
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(),
					Optional.of(event.getOperation().getVersion()));
			return;
		}
		if (event.getOperation().getOperationCode() == OperationCode.RESTORE_OBJECT_PREVIOUS_VERSION) {
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(), Optional.empty());
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(),
					Optional.of(event.getOperation().getVersion()));
			return;
		}
		if (event.getOperation().getOperationCode() == OperationCode.DELETE_OBJECT) {
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(), Optional.empty());
			return;
		}
		if (event.getOperation().getOperationCode() == OperationCode.DELETE_OBJECT_PREVIOUS_VERSIONS) {
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(), Optional.empty());
			if (getVirtualFileSystemService().getServerSettings().isVersionControl()) {
				for (int version = 0; version < event.getOperation().getVersion(); version++)
					remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(),
							Optional.of(version));
			}
			return;
		}
		if (event.getOperation().getOperationCode() == OperationCode.SYNC_OBJECT_NEW_DRIVE) {
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(), Optional.empty());
			return;
		}
		if (event.getOperation().getOperationCode() == OperationCode.INTEGRITY_CHECK) {
			remove(event.getOperation().getBucketId(), event.getOperation().getObjectName(), Optional.empty());
			return;
		}
	}

	@PostConstruct
	protected synchronized void onInitialize() {

		setStatus(ServiceStatus.STARTING);

		this.cache = Caffeine.newBuilder().initialCapacity(getServerSettings().getFileCacheInitialCapacity())
				.maximumSize(getServerSettings().getFileCacheMaxCapacity())
				.expireAfterWrite(getServerSettings().getFileCacheDurationDays(), TimeUnit.DAYS)
				.evictionListener((key, value, cause) -> {
					onRemoval(key, value, cause);
				}).removalListener((key, value, cause) -> {
					onRemoval(key, value, cause);
				}).build();
	}

	/**
	 * 
	 * <p>
	 * Deletes the cached File from the File System
	 * </p>
	 * 
	 * @param key
	 * @param value
	 * @param cause
	 */
	protected void onRemoval(Object key, Object value, RemovalCause cause) {
		if (cause.wasEvicted()) {
			if ((key != null) && (value != null)) {
				String pattern = Pattern.quote(File.separator);
				String arr[] = ((String) key).split(pattern);
				String verr[] = (arr[1].split(ServerConstant.BO_SEPARATOR));

				Long bucketId = Long.valueOf(arr[0]);
				String objectName = ((verr.length == 1) ? arr[1] : verr[0]);
				Optional<Integer> version = ((verr.length == 1) ? Optional.empty()
						: Optional.of(Integer.valueOf(verr[1]).intValue()));

				
				getLockService().getFileCacheLock(bucketId, objectName, version).writeLock().lock();
				try {
				
					logger.debug("delete cached file -> b:"+ bucketId.toString()+ " o:" + objectName +" v: "+String.valueOf(version));
					
					FileUtils.deleteQuietly((File) value);

					this.cacheSizeBytes.getAndAdd(-((File) value).length());
				} finally {
					getLockService().getFileCacheLock(bucketId, objectName, version).writeLock().unlock();
				}
			}
		}
	}

	private String getKey(Long bucketId, String objectName, Optional<Integer> version) {
		return bucketId.toString() + File.separator + objectName
				+ (version.isEmpty() ? "" : (ServerConstant.BO_SEPARATOR + String.valueOf(version.get().intValue())));
	}

	private synchronized List<Drive> getDrivesAll() {

		if (this.listDrives == null)
			this.listDrives = new ArrayList<Drive>(getVirtualFileSystemService().getMapDrivesAll().values());

		this.listDrives.sort(new Comparator<Drive>() {
			@Override
			public int compare(Drive o1, Drive o2) {
				try {
					if ((o1.getDriveInfo() == null))
						if (o2.getDriveInfo() != null)
							return 1;

					if ((o2.getDriveInfo() == null))
						if (o1.getDriveInfo() != null)
							return -1;

					if ((o1.getDriveInfo() == null) && o2.getDriveInfo() == null)
						return 0;

					if (o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder())
						return -1;

					if (o1.getDriveInfo().getOrder() > o2.getDriveInfo().getOrder())
						return 1;

					return 0;
				} catch (Exception e) {
					logger.error(e, SharedConstant.NOT_THROWN);
					return 0;
				}
			}
		});

		return this.listDrives;
	}

	private Cache<String, File> getCache() {
		return this.cache;
	}

	private ServerSettings getServerSettings() {
		return this.serverSettings;
	}

}
