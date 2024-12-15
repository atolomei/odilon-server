package io.odilon.virtualFileSystem;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.model.BaseObject;
import io.odilon.virtualFileSystem.model.ServerBucket;

public class BucketCache extends BaseObject {

    /**
     * <p>
     * this map contains accessible buckets, not marked as
     * {@code BucketState.DELETED} or {@code BucketState.DRAFT}
     * </p>
     */
    private Map<String, ServerBucket> buckets_by_name = new ConcurrentHashMap<String, ServerBucket>();

    @JsonIgnore
    private Map<Long, ServerBucket> buckets_by_id = new ConcurrentHashMap<Long, ServerBucket>();
    
    @JsonIgnore
    private ReentrantReadWriteLock bucketCacheLock = new ReentrantReadWriteLock();
    
    
    public BucketCache() {
        super();
    }
   
    public ServerBucket get(Long id) {
        return this.getIdMap().get(id);
    }
    
    public ServerBucket get(String name) {
        return this.getNameMap().get(name);
    }
    
    
    public void add(ServerBucket bucket) {

        this.bucketCacheLock.writeLock().lock();
        try {
            this.getIdMap().put(bucket.getId(), bucket);
            this.getNameMap().put(bucket.getName(), bucket);
        } finally {
            this.bucketCacheLock.writeLock().unlock();
        }
    }

    
    public void update(String oldBucketName, ServerBucket bucket) {
        this.bucketCacheLock.writeLock().lock();
        try {
            this.getNameMap().remove(oldBucketName);
            this.getNameMap().put(bucket.getName(), bucket);
            this.getIdMap().put(bucket.getId(), bucket);
        } finally {
            this.bucketCacheLock.writeLock().unlock();
        }
    }
    
    public void remove(Long id) {

        this.bucketCacheLock.writeLock().lock();
        try {
            if (this.getIdMap().containsKey(id)) {
                this.getNameMap().remove(this.getIdMap().get(id).getName());    
                this.getIdMap().remove(id);    
            }
        } finally {
            this.bucketCacheLock.writeLock().unlock();
        }
    }

    public void remove(String name) {

        this.bucketCacheLock.writeLock().lock();
        try {
            if (this.getNameMap().containsKey(name)) {
                this.getIdMap().remove(this.getNameMap().get(name).getId());
                this.getNameMap().remove(name);    
                    
            }
        } finally {
            this.bucketCacheLock.writeLock().unlock();
        }
    }

    
    protected Map<String, ServerBucket> getNameMap() {
        return this.buckets_by_name;
    }

    protected Map<Long, ServerBucket> getIdMap() {
        return buckets_by_id;
    }

    public boolean contains(String bucketName) {
        return getNameMap().containsKey(bucketName);
    }
    
    public boolean contains(Long id) {
        return getIdMap().containsKey(id);
    }
    
    public boolean contains(ServerBucket bucket) {
        return getIdMap().containsKey(bucket.getId());
    }
    
    
}
