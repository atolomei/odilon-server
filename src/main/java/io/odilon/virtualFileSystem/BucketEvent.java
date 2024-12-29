package io.odilon.virtualFileSystem;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.service.BaseEvent;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class BucketEvent extends BaseEvent {

    private static final long serialVersionUID = 1L;

    @JsonIgnore
    static private Logger logger = Logger.getLogger(BucketEvent.class.getName());

    private final ServerBucket bucket;

    public BucketEvent(VirtualFileSystemOperation operation, Action action, ServerBucket bucket) {
        super(operation, action);
        this.bucket = bucket;
    }

    public ServerBucket getBucket() {
        return this.bucket;
    }

}
