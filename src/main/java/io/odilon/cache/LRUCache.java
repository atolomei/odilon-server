package io.odilon.cache;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.model.ObjectMetadata;

public class LRUCache<K,V> {

	@JsonIgnore
	ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	@JsonIgnore
	private LinkedhashMapWithCapacity<K,V> map;

    public LRUCache(int capacity) {
        this.map = new LinkedhashMapWithCapacity<K,V>(capacity);
    }
    
    public void remove(K key) {
    	try {
    		lock.writeLock().lock();
    		map.remove(key);
    	} finally {
    		lock.writeLock().unlock();
    	}
    }
    
    public boolean containsKey(String key) {
    	try {
    		lock.readLock().lock();
    		return map.containsKey(key);
    	} finally {
    		lock.readLock().unlock();
    	}
    }

    public boolean containsValue(ObjectMetadata value) {
    	try {
    		lock.readLock().lock();
    		return map.containsValue(value);
    	} finally {
    		lock.readLock().unlock();
    	}
    }
    
    public boolean isEmpty() {
    	try {
    		lock.readLock().lock();
    		return map.isEmpty();
    	} finally {
    		lock.readLock().unlock();
    	}
    }

    public int size() {
    	try {
    		lock.readLock().lock();
    		return this.map.size();
    	} finally {
    		lock.readLock().unlock();
    	}
    	
    }
    
    /** 
	 * <p> we have to use writeLock instead of readLock because
	 	after getting the value, {@link LinkedHashMap#afterNodeAccess LinkedHashMap<K,V>.afterNodeAccess} 
	 	structurally affects the data container (moves the node the last)</p>
	 */
    public V get(K key) {
    	try {
    		lock.writeLock().lock();
        	return this.map.get(key);
    	} finally {
    		lock.writeLock().unlock();
    	}
    }

    public void put(K key, V value) {
    	try {
    		lock.writeLock().lock();
    		this.map.put(key, value);
    	} finally {
    		lock.writeLock().unlock();
    	}
    	
    	
    }
}