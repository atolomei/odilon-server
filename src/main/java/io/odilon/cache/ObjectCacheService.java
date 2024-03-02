package io.odilon.cache;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServiceStatus;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;

/**
 * 
 * 
 */
@Service
public class ObjectCacheService extends BaseService implements SystemService {
				
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
	@JsonIgnore
	private LRUCache<String, ObjectMetadata> map; 
	
	public ObjectCacheService(ServerSettings serverSettings) {
		this.serverSettings=serverSettings;
	}
	
    public int size() {
        return this.map.size(); 	
    }

    public boolean isEmpty() {
    	return this.map.isEmpty();
    }
    
    public boolean containsKey(String bucketName, String objectName) {
    	return this.map.containsKey(getKey(bucketName, objectName));
    }

    public boolean containsValue(ObjectMetadata value) {
    	return this.map.containsValue(value);
    }

    public ObjectMetadata get(String bucketName, String objectName) {
    	return this.map.get(getKey(bucketName, objectName));
    }

    public void put(String bucketName, String objectName, ObjectMetadata value) {
    	this.map.put(getKey(bucketName, objectName), value);
    }

    public void remove(String bucketName, String objectName) {
    	this.map.remove(getKey(bucketName, objectName));
    }
    
    @PostConstruct
	protected void onInitialize() {
		try {
			setStatus(ServiceStatus.STARTING);
			this.map = new LRUCache<String, ObjectMetadata>(this.serverSettings.getObjectCacheCapacity());
		} finally {
			setStatus(ServiceStatus.RUNNING);
	 		startuplogger.debug("Started -> " + ObjectCacheService.class.getSimpleName());
		}
	}
	
    /**
     * character '>' is not a valid bucket or object name
     * 
     * @param bucketName
     * @param objectName
     * @return
     */
    private String getKey( String bucketName, String objectName) {
    	return bucketName+">"+objectName;
    }
    
}


