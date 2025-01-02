package io.odilon.virtualFileSystem.raid0;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public abstract class RAIDZeroRollbackObjectHandler extends RAIDZeroRollbackHandler {

    @JsonIgnore
    private ObjectPath path;
    
    public RAIDZeroRollbackObjectHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
        super(driver, operation, recovery);
    }

    
    protected ObjectPath getObjectPath() {
        if (this.path==null) {
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            String objectName = getOperation().getObjectName();
            this.path= new ObjectPath(getWriteDrive(bucket, objectName), bucket, objectName);
        }
        return this.path;
    }
    
    


}
