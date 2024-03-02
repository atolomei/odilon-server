package io.odilon.cache;

import java.util.LinkedHashMap;
import java.util.Map;


class LinkedhashMapWithCapacity<K,V> extends LinkedHashMap<K,V> {
    
	private static final long serialVersionUID = 1L;
	
	private int capacity;

    public LinkedhashMapWithCapacity(int capacity) {
        super(64, 0.75f, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
        return this.size() > this.capacity;
    }
}

