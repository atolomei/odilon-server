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

        switch (getOperation().getOperationCode()) {
        case SYNC_OBJECT_NEW_DRIVE: {
            execRollback(getOperation(), isRecovery());
            break;
        }
        default: {
            break;
        }
        }
    }

    private void execRollback(VirtualFileSystemOperation operation, boolean recoveryMode) {

        boolean done = false;

        String objectName = operation.getObjectName();

        ServerBucket bucket = null;

        getLockService().getObjectLock(operation.getBucketId(), objectName).writeLock().lock();
        try {

            getLockService().getBucketLock(operation.getBucketId()).readLock().lock();
            try {
                bucket = getBucketCache().get(operation.getBucketId());
                restoreMetadata(bucket, objectName);
                done = true;

            } catch (InternalCriticalException e) {
                if (!recoveryMode)
                    throw (e);
                else
                    logger.error(opInfo(operation), SharedConstant.NOT_THROWN);
            } catch (Exception e) {
                if (!recoveryMode)
                    throw new InternalCriticalException(e, opInfo(operation));
                else
                    logger.error(e, opInfo(operation), SharedConstant.NOT_THROWN);
            } finally {
                try {
                    if (done || recoveryMode) {
                        operation.cancel();
                    }
                } finally {
                    getLockService().getBucketLock(bucket).readLock().unlock();
                }
            }
        } finally {
            getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
        }
    }

    private void restoreMetadata(ServerBucket bucket, String objectName) {
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
