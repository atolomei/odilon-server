package io.odilon.cache;

import java.io.File;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServiceStatus;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;

/**
 * <p>{@link ObjectMetadata} cache.</p> 
 * It Uses {@link Caffeine} to keep references to entries in memory.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class ObjectCacheService extends BaseService implements SystemService {
				
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	static final int INITIAL_CAPACITY = 10000;
	static final int MAX_SIZE = 500000;
	static final int TIMEOUT_DAYS = 7;

	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
	@JsonIgnore
	private Cache<String, ObjectMetadata> cache;
	

	public ObjectCacheService(ServerSettings serverSettings) {
		this.serverSettings=serverSettings;
	}
	
    public long size() {
        return this.cache.estimatedSize(); 	
    }
    
    public boolean containsKey(String bucketName, String objectName) {
    	return (this.cache.getIfPresent(getKey(bucketName, objectName))!=null);
    }

    public ObjectMetadata get(String bucketName, String objectName) {
    	return this.cache.getIfPresent(getKey(bucketName, objectName));
    }

    public void put(String bucketName, String objectName, ObjectMetadata value) {
    	this.cache.put(getKey(bucketName, objectName), value);
    }

    public void remove(String bucketName, String objectName) {
    	this.cache.invalidate(getKey(bucketName, objectName));
    }
    
    @PostConstruct
	protected void onInitialize() {
		try {
			setStatus(ServiceStatus.STARTING);
			this.cache = Caffeine.newBuilder()
						.initialCapacity(INITIAL_CAPACITY)    
						.maximumSize(MAX_SIZE)
						.expireAfterWrite(TIMEOUT_DAYS, TimeUnit.DAYS)
						.build();
			
		} finally {
			setStatus(ServiceStatus.RUNNING);
	 		startuplogger.debug("Started -> " + ObjectCacheService.class.getSimpleName());
		}
	}
	
    /**
     * File.separator is not a valid bucket or object name
     * 
     * @param bucketName
     * @param objectName
     * @return
     */
    private String getKey( String bucketName, String objectName) {
    	return bucketName+File.separator+objectName;
    }
    
}


