package io.odilon.virtualFileSystem.raid6;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.cache.FileCacheService;
import io.odilon.model.BaseObject;
import io.odilon.model.ObjectMetadata;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

public class RAIDSixCoder extends BaseObject {
    

    @JsonIgnore
    private final RAIDSixDriver driver;
    
    
    protected RAIDSixCoder(RAIDSixDriver driver) {
        Check.requireNonNull(driver);
        this.driver = driver;
    }
    
   protected VirtualFileSystemService getVirtualFileSystemService() {
        return getDriver().getVirtualFileSystemService();
    }
    
    protected String objectInfo(ObjectMetadata meta) {
        return getDriver().objectInfo(meta);
    }
    
    protected FileCacheService getFileCacheService() {
        return getVirtualFileSystemService().getFileCacheService();
    }

    protected ServerBucket getBucketById(Long id) {
        return getVirtualFileSystemService().getBucketById(id);
    }
    
    public RAIDSixDriver getDriver() {
        return this.driver;
    }
}
