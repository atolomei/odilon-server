/*
 * Odilon Object Storage
 * (C) Novamens 
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

import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServiceStatus;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.vfs.model.VFSOp;

/**
 * <p>{@link ObjectMetadata} Cache. It only stores ObjectMetadata's <b>head version</b></p> 
 * <p>It Uses {@link Caffeine} to keep references to entries in memory
 * (<a href="https://github.com/ben-manes/caffeine">Caffeine on GitHub).</a>
 * </p> 
 *  
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 * 
 */
@Service
public class ObjectMetadataCacheService extends BaseService implements ApplicationListener<CacheEvent>  {
				
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	static private Logger logger = Logger.getLogger(ObjectMetadataCacheService.class.getName());

	static final int INITIAL_CAPACITY = 10000;

	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	@JsonIgnore
	private Cache<String, ObjectMetadata> cache;

	
	public ObjectMetadataCacheService(ServerSettings serverSettings) {
		this.serverSettings=serverSettings;
	}
	
    public long size() {
        return getCache().estimatedSize(); 	
    }
    
    public boolean containsKey(Long bucketId, String objectName) {
    	return (getCache().getIfPresent(getKey(bucketId, objectName))!=null);
    }

    public ObjectMetadata get(Long bucketId, String objectName) {
    	return getCache().getIfPresent(getKey(bucketId, objectName));
    }

    public void put(Long bucketId, String objectName, ObjectMetadata value) {
    	getCache().put(getKey(bucketId, objectName), value);
    }

    public void remove(Long bucketId, String objectName) {
    	getCache().invalidate(getKey(bucketId, objectName));
    }
    
    
    /**
     * <p>fired by {@link JournalService} on commit or cance</p>
     */
    @Override
	public void onApplicationEvent(CacheEvent event) {
		
    	if (event.getVFSOperation()==null) {
    		logger.error("event Operation is null ");
    		return;
    	}
    	if (event.getVFSOperation().getOp()==null) {
    		logger.debug("op is null -> "  + event.toString());
    		return;
    	}
    	
    	
		if (event.getVFSOperation().getOp()==VFSOp.CREATE_OBJECT) {
			remove(event.getVFSOperation().getBucketId(), event.getVFSOperation().getObjectName());
			return;
		}
		if (event.getVFSOperation().getOp()==VFSOp.UPDATE_OBJECT) {
			remove(event.getVFSOperation().getBucketId(), event.getVFSOperation().getObjectName());
			return;
		}
		if (event.getVFSOperation().getOp()==VFSOp.RESTORE_OBJECT_PREVIOUS_VERSION) {
			remove(event.getVFSOperation().getBucketId(), event.getVFSOperation().getObjectName());
			return;
		}
		if (event.getVFSOperation().getOp()==VFSOp.DELETE_OBJECT) {
			remove(event.getVFSOperation().getBucketId(), event.getVFSOperation().getObjectName());
			return;
		}
		
		if (event.getVFSOperation().getOp()==VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS) {
			remove(event.getVFSOperation().getBucketId(), event.getVFSOperation().getObjectName());
			return;
		}
		
		if (event.getVFSOperation().getOp()==VFSOp.SYNC_OBJECT_NEW_DRIVE) {
			remove(event.getVFSOperation().getBucketId(), event.getVFSOperation().getObjectName());
			return;
		}
    }

		
    @PostConstruct
	protected void onInitialize() {
		try {
			setStatus(ServiceStatus.STARTING);
			
			this.cache = Caffeine.newBuilder()
						.initialCapacity(INITIAL_CAPACITY)    
						.maximumSize(serverSettings.getObjectCacheCapacity())
						.expireAfterWrite(serverSettings.getObjectCacheExpireDays(), TimeUnit.DAYS)
						.build();
			
		} finally {
			setStatus(ServiceStatus.RUNNING);
	 		startuplogger.debug("Started -> " + ObjectMetadataCacheService.class.getSimpleName());
		}
	}
	
    /**
     * File.separator is not a valid bucket or object name
     * 
     * @param bucketName
     * @param objectName
     * @return
     */
    private String getKey(Long bucketId, String objectName) {
    	return bucketId.toString() + File.separator + objectName;
    }
    
    private Cache<String, ObjectMetadata> getCache() {
		return this.cache;
    }
    
}


