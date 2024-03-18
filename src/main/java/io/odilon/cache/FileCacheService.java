package io.odilon.cache;

import java.io.File;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.ThreadSafe;

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
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VirtualFileSystemService;


/**
 * <p>{@link File} cache used by {@link RAIDSixDriver} (the other RAID configurations do not need it).</p> 
 * It Uses {@link Caffeine} to keep references to entries in memory.
 * On eviction it has to remove the {@link File} from the File System ({@link FileCacheService#onRemoval}).</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
@Service
public class FileCacheService extends BaseService {
			
	static private Logger logger = Logger.getLogger(FileCacheService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	static final int INITIAL_CAPACITY = 100;
	static final int MAX_SIZE = 25000;
	static final int TIMEOUT_DAYS = 7;

	private AtomicLong cacheSizeBytes = new AtomicLong(0);
	
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
	

    public boolean containsKey(String bucketName, String objectName, Optional<Integer> version) {
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);
		getLockService().getFileCacheLock(bucketName, objectName, version).readLock().lock();
		try {
    		return (this.cache.getIfPresent(getKey(bucketName, objectName, version))!=null);
    	} finally {
    		getLockService().getFileCacheLock(bucketName, objectName, version).readLock().unlock();
    	}
    }

    /**
     * @param bucketName
     * @param objectName
     * @return
     */
    public File get(String bucketName, String objectName, Optional<Integer> version) {
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);
		getLockService().getFileCacheLock(bucketName, objectName, version).readLock().lock();
    	try {
    		return this.cache.getIfPresent(getKey(bucketName, objectName, version));
    	} finally {
    		getLockService().getFileCacheLock(bucketName, objectName, version).readLock().unlock();
    	}
    }

    /**
     * <p>If the lock was set by the calling object, we do not apply it here </p>
     * @param bucketName
     * @param objectName
     * @param file
     * @return
     */
    public void put(String bucketName, String objectName, Optional<Integer> version, File file, boolean lockRequired) {
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);
		Check.requireNonNullArgument(file, "file is null");
		if (lockRequired)
			getLockService().getFileCacheLock(bucketName, objectName, version).writeLock().lock();
		try {
			this.cache.put(getKey(bucketName, objectName, version), file);
			cacheSizeBytes.getAndAdd(file.length());
    	} finally {
    		if (lockRequired)
    			getLockService().getFileCacheLock(bucketName, objectName, version).writeLock().unlock();
    	}
    }
    /**
     * @param bucketName
     * @param objectName
     */
    public void remove(String bucketName, String objectName, Optional<Integer> version) {
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);
		getLockService().getFileCacheLock(bucketName, objectName, version).writeLock().lock();
    	try {
    		File file = this.cache.getIfPresent(getKey(bucketName, objectName, version));
    		this.cache.invalidate(getKey(bucketName, objectName, version));
    		if (file!=null) {
    			FileUtils.deleteQuietly(file);
    			cacheSizeBytes.getAndAdd(-file.length());
    		}
    	} finally {
    		getLockService().getFileCacheLock(bucketName, objectName, version).writeLock().unlock();
    	}
    }

    /**
     * @param bucketName
     * @param objectName
     * @return
     */
	public String getFileCachePath(String bucketName, String objectName, Optional<Integer> version) {
		String path = getKey(bucketName, objectName, version);
		return getDrivesAll().get(Math.abs(path.hashCode()) % getDrivesAll().size()).getCacheDirPath() + File.separator + path;
	}
	
    public long size() {
   		return this.cache.estimatedSize();
    }
    
    public long hardDiskUsage() {
   		return this.cacheSizeBytes.get();
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
			logger.error("The instance of " + VirtualFileSystemService.class.getSimpleName() + " must be setted during the @PostConstruct method of the " + this.getClass().getName() + " instance. It can not be injected via AutoWired beacause of circular dependencies.");
			throw new IllegalStateException(VirtualFileSystemService.class.getSimpleName() + " instance is null. it must be setted during the @PostConstruct method of the " + this.getClass().getName() + " instance");
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
	 * 
	 * <p>Deletes the cached File from the File System</p>
	 * 
	 * @param key
	 * @param value
	 * @param cause
	 */
	protected void onRemoval(@Nullable Object key, @Nullable Object value, @NonNull RemovalCause cause) {
		if (cause.wasEvicted()) {
			if ((key!=null) && (value!=null)) {
				String pattern = Pattern.quote(File.separator);
				String  arr[]  = ((String)key).split(pattern);
				String verr[] = (arr[1].split(ServerConstant.BO_SEPARATOR));
				
				String bucketName = arr[0];
				String objectName = ((verr.length==1) ? arr[1] : verr[0]);
				Optional<Integer> version = ((verr.length==1) ? Optional.empty() : Optional.of(Integer.valueOf(verr[1]).intValue()));

				getLockService().getFileCacheLock(bucketName, objectName, version).writeLock().lock();
				try {
		    			FileUtils.deleteQuietly((File) value);
		    			cacheSizeBytes.getAndAdd(-((File)value).length());
				} finally {
					getLockService().getFileCacheLock(bucketName, objectName, version).writeLock().unlock();
				}
			}
		}
	}
	
    private String getKey(String bucketName, String objectName, Optional<Integer> version) {
    	return bucketName + File.separator + objectName + (version.isEmpty()?"":(ServerConstant.BO_SEPARATOR+String.valueOf(version.get().intValue())));
    }
    
    
	private synchronized List<Drive> getDrivesAll() {
		
		if (this.listDrives ==null)
			this.listDrives = new ArrayList<Drive>(getVFS().getMapDrivesAll().values());
		
		this.listDrives.sort(new Comparator<Drive>() {
			@Override
			public int compare(Drive o1, Drive o2) {
				try {
						if ((o1.getDriveInfo()==null))
							if (o2.getDriveInfo()!=null) return 1;
						
						if ((o2.getDriveInfo()==null))
							if (o1.getDriveInfo()!=null) return -1;
						
						if ((o1.getDriveInfo()==null) && o2.getDriveInfo()==null)
							return 0;
						
						if (o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder())
							return -1;
						
						if (o1.getDriveInfo().getOrder() > o2.getDriveInfo().getOrder())
							return 1;
							
						return 0;
					}
						catch (Exception e) {
							return 0;		
					}
				}
		});

		return this.listDrives;
	}

	
}
