package io.odilon.virtualFileSystem;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.model.BaseObject;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.ServerBucket;

public class BucketCache extends BaseObject {

    /**
     * <p>
     * this cache contains accessible buckets, ie not marked as
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
        Check.requireNonNullArgument(id, "id is null");
        return this.getIdMap().get(id);
    }

    public ServerBucket get(String name) {
        Check.requireNonNullArgument(name, "name is null");
        return this.getNameMap().get(name);
    }

    public void add(ServerBucket bucketToAdd) {
        Check.requireNonNullArgument(bucketToAdd, "bucket is null");
        ServerBucket serverBucket = null;
        lock();
        try {
            if (getIdMap().containsKey(bucketToAdd.getId()))
                serverBucket = getIdMap().get(bucketToAdd.getId());
            this.getIdMap().put(bucketToAdd.getId(), bucketToAdd);
            this.getNameMap().put(bucketToAdd.getName(), bucketToAdd);
        } catch (Exception e) {
            restore(serverBucket, bucketToAdd);
            throw (e);
        } finally {
            unLock();
        }
    }

    public void update(String oldBucketName, ServerBucket bucketToUpdate) {
        Check.requireNonNullArgument(bucketToUpdate, "bucket is null");
        ServerBucket serverBucket = null;
        lock();
        try {
            if (getIdMap().containsKey(bucketToUpdate.getId()))
                serverBucket = getIdMap().get(bucketToUpdate.getId());
            this.getNameMap().remove(oldBucketName);
            this.getNameMap().put(bucketToUpdate.getName(), bucketToUpdate);
            this.getIdMap().put(bucketToUpdate.getId(), bucketToUpdate);
        } catch (Exception e) {
            restore(serverBucket, bucketToUpdate);
            throw (e);
        } finally {
            unLock();
        }
    }

    public void remove(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        remove(bucket.getId());
    }
    
    public void remove(Long id) {
        Check.requireNonNullArgument(id, "bucket is null");
        ServerBucket serverBucket = null;
        lock();
        try {
            if (this.getIdMap().containsKey(id)) {
                serverBucket = getIdMap().get(id);
                this.getNameMap().remove(this.getIdMap().get(id).getName());
                this.getIdMap().remove(id);
            }
        } catch (Exception e) {
            restore(serverBucket, null);
            throw (e);
        } finally {
            unLock();
        }
    }

    public void remove(String name) {
        Check.requireNonNullArgument(name, "name is null");
        ServerBucket serverBucket = null;
        lock();
        try {
            if (this.getNameMap().containsKey(name)) {
                serverBucket = getNameMap().get(name);
                this.getIdMap().remove(this.getNameMap().get(name).getId());
                this.getNameMap().remove(name);
            }
        } catch (Exception e) {
            restore(serverBucket, null);
            throw (e);
        } finally {
            unLock();
        }
    }

    public boolean contains(String bucketName) {
        Check.requireNonNullArgument(bucketName, "name is null");
        return getNameMap().containsKey(bucketName);
    }

    public boolean contains(Long id) {
        Check.requireNonNullArgument(id, "id is null");
        return getIdMap().containsKey(id);
    }

    public boolean contains(ServerBucket bucket) {
        Check.requireNonNullArgument(bucket, "bucket is null");
        return getIdMap().containsKey(bucket.getId());
    }

    protected Map<String, ServerBucket> getNameMap() {
        return this.buckets_by_name;
    }

    protected Map<Long, ServerBucket> getIdMap() {
        return buckets_by_id;
    }

    private void restore(ServerBucket oldItem, ServerBucket newItem) {
        if (newItem != null) {
            this.getIdMap().remove(newItem.getId());
            this.getNameMap().remove(newItem.getName());
        }
        if (oldItem != null) {
            this.getIdMap().put(oldItem.getId(), oldItem);
            this.getNameMap().put(oldItem.getName(), oldItem);
        }
    }

    private void lock() {
        bucketCacheLock().writeLock().lock();
    }

    private void unLock() {
        bucketCacheLock().writeLock().unlock();
    }

    private ReentrantReadWriteLock bucketCacheLock() {
        return this.bucketCacheLock;
    }

}
