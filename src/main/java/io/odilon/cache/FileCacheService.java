package io.odilon.cache;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
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
	
	@JsonIgnore
	@Autowired
	private final LockService vfsLockService;
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
	/** lazy injection */
	@JsonIgnore
	private VirtualFileSystemService vfs;

	@JsonIgnore
	private LRUCache<String, File> map; 
	
	@JsonIgnore
	private List<Drive> listDrives;
	
	public LockService getLockService() {
		return this.vfsLockService;
	}
	
	public FileCacheService(ServerSettings serverSettings, LockService vfsLockService) {
		this.serverSettings=serverSettings;
		this.vfsLockService=vfsLockService;
	}
	
	public String getFileCachePath(String bucketName, String objectName) {
		String path = getKey(bucketName, objectName);
		Drive drive = getDrivesAll().get(Math.abs(path.hashCode() % getDrivesAll().size()));
		return drive.getWorkDirPath() + File.separator + path;
		
	}
    public int size() {
   		return map.size();
    }

    public boolean isEmpty() {
   		return map.isEmpty();
    }
    
    public boolean containsKey(String bucketName, String objectName) {
    	
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);

		getLockService().getFileCacheLock(bucketName, objectName).readLock().lock();

		try {
    		return map.containsKey(getKey(bucketName, objectName));
    	} finally {
    		getLockService().getFileCacheLock(bucketName, objectName).readLock().unlock();
    	}
    }

    /**
     * 
     * @param bucketName
     * @param objectName
     * @return
     */
    public File get(String bucketName, String objectName) {

    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);

		getLockService().getFileCacheLock(bucketName, objectName).readLock().lock();
		
    	try {
    		return map.get(getKey(bucketName, objectName));
    		
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

			map.put(getKey(bucketName, objectName), file);
			
    	} finally {
    		
    		if (lockRequired)
    			getLockService().getFileCacheLock(bucketName, objectName).writeLock().unlock();
    	}
    	
    }

    /**
     * 
     * @param bucketName
     * @param objectName
     */
    public void remove(String bucketName, String objectName) {
    	
    	Check.requireNonNullArgument(bucketName, "bucket is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null | b:" + bucketName);

		getLockService().getFileCacheLock(bucketName, objectName).writeLock().lock();
		
    	try {
    		
    		File file = map.get(getKey(bucketName, objectName));
	    	
	    	try {
	    		
	    		if (file!=null && file.exists())
	    			FileUtils.forceDelete(file);
	    		
			} catch (IOException e) {
				logger.error(e);
				throw new InternalCriticalException(e);
			}
	    	map.remove(getKey(bucketName, objectName));
	    	
    	} finally {
    		
    		getLockService().getFileCacheLock(bucketName, objectName).writeLock().unlock();
    		
    	}
    }

    
	public synchronized void setVFS(VirtualFileSystemService virtualFileSystemService) {
		this.vfs=virtualFileSystemService;
	}
	
	public VirtualFileSystemService getVFS() {
		if (this.vfs==null) {
			logger.error("The " + VirtualFileSystemService.class.getName() + " must be setted during the @PostConstruct method of the " + this.getClass().getName() + " instance. It can not be injected via AutoWired beacause of circular dependencies.");
			throw new IllegalStateException(VirtualFileSystemService.class.getName() + " is null. it must be setted during the @PostConstruct method of the " + this.getClass().getName() + " instance");
		}
		return this.vfs;
	}
	
	public synchronized List<Drive> getDrivesAll() {
		if (this.listDrives ==null)
			this.listDrives = new ArrayList<Drive>(getVFS().getDrivesAll().values());
		return this.listDrives;
	}

	@PostConstruct
	protected synchronized void onInitialize() {
		
		try {
			setStatus(ServiceStatus.STARTING);
			this.map = new LRUCache<String, File>(this.serverSettings.getFileCacheCapacity());
			
		} finally {
			setStatus(ServiceStatus.RUNNING);
	 		startuplogger.debug("Started -> " + FileCacheService.class.getSimpleName());
		}
	}

    private String getKey( String bucketName, String objectName) {
    	return bucketName+"-"+objectName;
    }
	
}
