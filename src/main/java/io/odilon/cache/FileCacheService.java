package io.odilon.cache;

import java.io.File;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ServiceStatus;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VirtualFileSystemService;


@Service
public class FileCacheService extends BaseService {
			
	static private Logger logger = Logger.getLogger(FileCacheService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	static final int INITIAL_CAPACITY = 100;
	static final int MAX_SIZE = 25000;
	static final int TIMEOUT_DAYS = 7;

	private long cacheSizeBytes = 0;
	
	@JsonIgnore
	@Autowired
	private final LockService vfsLockService;
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
	/** lazy injection due to circular dependencies */
	@JsonIgnore
	private VirtualFileSystemService vfs;

	@JsonIgnore
	private List<Drive> listDrives;

	@JsonIgnore
	private Cache<String, File> cache;
	
	
	public FileCacheService(ServerSettings serverSettings, LockService vfsLockService) {
		this.serverSettings=serverSettings;
		this.vfsLockService=vfsLockService;
	}
	

    public boolean containsKey(String bucketName, String objectName) {
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);
		getLockService().getFileCacheLock(bucketName, objectName).readLock().lock();
		try {
    		return (this.cache.getIfPresent(getKey(bucketName, objectName))!=null);
    	} finally {
    		getLockService().getFileCacheLock(bucketName, objectName).readLock().unlock();
    	}
    }

    /**
     * @param bucketName
     * @param objectName
     * @return
     */
    public File get(String bucketName, String objectName) {
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);
		getLockService().getFileCacheLock(bucketName, objectName).readLock().lock();
    	try {
    		return this.cache.getIfPresent(getKey(bucketName, objectName));
    	} finally {
    		getLockService().getFileCacheLock(bucketName, objectName).readLock().unlock();
    	}
    }

    /**
     * <p>If the lock was set by the calling object, we do not apply it here </p>
     * @param bucketName
     * @param objectName
     * @param file
     * @return
     */
    public void put(String bucketName, String objectName, File file, boolean lockRequired) {
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);
		Check.requireNonNullArgument(file, "file is null");
		if (lockRequired)
			getLockService().getFileCacheLock(bucketName, objectName).writeLock().lock();
		try {
			this.cache.put(getKey(bucketName, objectName), file);
			cacheSizeBytes += file.length();
    	} finally {
    		if (lockRequired)
    			getLockService().getFileCacheLock(bucketName, objectName).writeLock().unlock();
    	}
    }
    /**
     * @param bucketName
     * @param objectName
     */
    public void remove(String bucketName, String objectName) {
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);
		getLockService().getFileCacheLock(bucketName, objectName).writeLock().lock();
    	try {
    		File file = this.cache.getIfPresent(getKey(bucketName, objectName));
    		this.cache.invalidate(getKey(bucketName, objectName));
    		if (file!=null) {
    			FileUtils.deleteQuietly(file);
    			cacheSizeBytes -= file.length();
    		}
    	} finally {
    		getLockService().getFileCacheLock(bucketName, objectName).writeLock().unlock();
    	}
    }

    /**
     * 
     * @param bucketName
     * @param objectName
     * @return
     */
	public String getFileCachePath(String bucketName, String objectName) {
		String path = getKey(bucketName, objectName);
		return getDrivesAll().get(Math.abs(path.hashCode()) % getDrivesAll().size()).getCacheDirPath() + File.separator + path;
	}
	
    public long size() {
   		return this.cache.estimatedSize();
    }
    
    public long hardDiskUsage() {
   		return this.cacheSizeBytes;
    }

    
    public synchronized void setVFS(VirtualFileSystemService virtualFileSystemService) {
		try {
	    	this.vfs=virtualFileSystemService;
    	} finally {
			setStatus(ServiceStatus.RUNNING);
	 		startuplogger.debug("Started -> " + FileCacheService.class.getSimpleName());
    	}
	}
	
	public VirtualFileSystemService getVFS() {
		if (this.vfs==null) {
			logger.error("The " + VirtualFileSystemService.class.getName() + " must be setted during the @PostConstruct method of the " + this.getClass().getName() + " instance. It can not be injected via AutoWired beacause of circular dependencies.");
			throw new IllegalStateException(VirtualFileSystemService.class.getName() + " is null. it must be setted during the @PostConstruct method of the " + this.getClass().getName() + " instance");
		}
		return this.vfs;
	}

	public LockService getLockService() {
		return this.vfsLockService;
	}
	
	@PostConstruct
	protected synchronized void onInitialize() {
		
		setStatus(ServiceStatus.STARTING);
		
		this.cache = Caffeine.newBuilder()
					.initialCapacity(INITIAL_CAPACITY)    
					.maximumSize(this.serverSettings.getFileCacheCapacity())
				    .expireAfterWrite(this.serverSettings.getFileCacheDurationDays(), TimeUnit.DAYS)
				    .evictionListener((key, value, cause) -> {
		            	onRemoval(key, value, cause);
				    })
				    .removalListener((key, value, cause) -> {
			            onRemoval(key, value, cause);
			        })
				    .build();
	}

	
	/**
	 * @param key
	 * @param value
	 * @param cause
	 */
	protected void onRemoval(@Nullable Object key, @Nullable Object value, @NonNull RemovalCause cause) {
		if (cause.wasEvicted()) {
			if ((key!=null) && (value!=null)) {
				String pattern = Pattern.quote(File.separator);
				String arr[] = ((String)key).split(pattern);
				getLockService().getFileCacheLock(arr[0], arr[1]).writeLock().lock();
				try {
		    			FileUtils.deleteQuietly((File) value);
		    			cacheSizeBytes -= ((File)value).length();
				} finally {
					getLockService().getFileCacheLock(arr[0], arr[1]).writeLock().unlock();
				}
			}
		}
	}
	
    private String getKey(String bucketName, String objectName) {
    	return bucketName + File.separator + objectName;
    }
    
	private List<Drive> getDrivesAll() {
		if (this.listDrives ==null)
			this.listDrives = new ArrayList<Drive>(getVFS().getDrivesAll().values());
		return this.listDrives;
	}

	
}
