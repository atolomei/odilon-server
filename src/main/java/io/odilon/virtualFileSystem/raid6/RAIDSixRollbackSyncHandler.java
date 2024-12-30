package io.odilon.virtualFileSystem.raid6;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDSixRollbackSyncHandler extends RAIDSixRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDSixRollbackSyncHandler.class.getName());

    public RAIDSixRollbackSyncHandler(RAIDSixDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {
        
        boolean done = false;

        String objectName = getOperation().getObjectName();


        getLockService().getObjectLock(getOperation().getBucketId(), getOperation().getObjectName()).writeLock().lock();
        try {

            getLockService().getBucketLock(getOperation().getBucketId()).readLock().lock();
            try {
                restoreMetadata();
                done = true;

            } catch (InternalCriticalException e) {
                if (!isRecovery())
                    throw (e);
                else
                    logger.error(opInfo(getOperation()), SharedConstant.NOT_THROWN);
            } catch (Exception e) {
                if (!isRecovery())
                    throw new InternalCriticalException(e, opInfo(getOperation()));
                else
                    logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
            } finally {
                try {
                    if (done || isRecovery()) {
                        getOperation().cancel();
                    }
                } finally {
                    getLockService().getBucketLock(getOperation().getBucketId()).readLock().unlock();
                }
            }
        } finally {
            getLockService().getObjectLock(getOperation().getBucketId(), objectName).writeLock().unlock();
        }
    }

    private void restoreMetadata() {
        
        ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
        String objectName = getOperation().getObjectName();
        
        try {
            for (Drive drive : getDriver().getDrivesEnabled()) {
                File dest = new File(drive.getObjectMetadataDirPath(bucket, objectName));
                File src = new File(drive.getBucketWorkDirPath(bucket) + File.separator + objectName);
                if (src.exists())
                    FileUtils.copyDirectory(src, dest);
                else
                    throw new InternalCriticalException("backup dir does not exist " + getDriver().objectInfo(bucket, objectName)
                            + "dir:" + src.getAbsolutePath());
            }
        } catch (IOException e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
        }
    }
}
