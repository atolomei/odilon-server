package io.odilon.cache;

import org.springframework.stereotype.Service;

import io.odilon.model.BaseService;
import io.odilon.model.ObjectMetadata;
import io.odilon.service.SystemService;


/**
 * 
 * 
 * 
 */
@Service
public class ObjectCacheService extends BaseService implements SystemService {
							
	static private final long defaultMaxLifeTimeMillis = 1000 * 60 * 60 * 24 * 1; // 1d
						
	SelfExpiringHashMap<String, ObjectMetadata> map = new SelfExpiringHashMap<String, ObjectMetadata>(defaultMaxLifeTimeMillis);
	
	
	public ObjectCacheService() {
	}
	
	/**
     * {@inheritDoc}
     */
    public int size() {
        return map.size(); 	
    }

    /**
     * {@inheritDoc}
     */
    
    public boolean isEmpty() {
    	return map.isEmpty();
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean containsKey(String bucketName, String objectName) {
    	return map.containsKey(getKey(bucketName, objectName));
    }

    public boolean containsValue(ObjectMetadata value) {
    	return map.containsValue(value);
    }

    public ObjectMetadata get(String bucketName, String objectName) {
    	return map.get(getKey(bucketName, objectName));
    }

    public ObjectMetadata put(String bucketName, String objectName, ObjectMetadata value) {
    	return map.put(getKey(bucketName, objectName), value);
    }

    public ObjectMetadata put(String bucketName, String objectName, ObjectMetadata value, long lifeTimeMillis) {
       	return map.put(getKey(bucketName, objectName), value, lifeTimeMillis);
    }

    public ObjectMetadata remove(String bucketName, String objectName) {
    	return map.remove(getKey(bucketName, objectName));
    }
    
    public boolean renewKey(String bucketName, String objectName) {
    	return map.renewKey(getKey(bucketName, objectName));
    }
    
    private String getKey( String bucketName, String objectName) {
    	return bucketName+">"+objectName;
    }
}
