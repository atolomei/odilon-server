package io.odilon.virtualFileSystem.raid1;

import java.io.File;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;

import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDOneRollbackUpdateHandler extends RAIDOneRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDOneRollbackUpdateHandler.class.getName());

    public RAIDOneRollbackUpdateHandler(RAIDOneDriver driver, VirtualFileSystemOperation operation, boolean isRecovery) {
        super(driver, operation, isRecovery);
    }

    @Override
    protected void rollback() {

        if (getOperation() == null)
            return;

        if (getServerSettings().isStandByEnabled())
            getReplicationService().cancel(getOperation());
        
        if (getOperation().getOperationCode() == OperationCode.UPDATE_OBJECT)
            rollbackUpdate();

        else if (getOperation().getOperationCode() == OperationCode.UPDATE_OBJECT_METADATA)
            rollbackUpdateMetadata();

        else if (getOperation().getOperationCode() == OperationCode.RESTORE_OBJECT_PREVIOUS_VERSION)
            rollbackUpdate();
    }

    private void rollbackUpdate() {

        boolean done = false;
        try {
            restoreVersionObjectDataFile();
            restoreVersionObjectMetadata();
            done = true;
        } catch (InternalCriticalException e) {
            logger.error(getDriver().opInfo(getOperation()));
            if (!isRecovery())
                throw (e);
        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, opInfo(getOperation()));
            else
                logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery()) {
                getOperation().cancel();
            }
        }
    }
    
    private void rollbackUpdateMetadata() {

        boolean done = false;
        try {
             restoreVersionObjectMetadata();
            done = true;
        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, opInfo(getOperation()));
            else
                logger.error(opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery()) {
                getOperation().cancel();
            }
        }
    }

    private boolean restoreVersionObjectDataFile() {

        ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
        
        try {
            boolean success = true;
            for (Drive drive : getDriver().getDrivesAll()) {
                ObjectPath path = new ObjectPath(drive, bucket, getOperation().getObjectName());
                File file = path.dataFileVersionPath(getOperation().getVersion()).toFile();
                // File file = ((SimpleDrive) drive).getObjectDataVersionFile(bucket.getId(),
                // objectName, version);
                if (file.exists()) {
                    ((SimpleDrive) drive).putObjectDataFile(bucket.getId(), getOperation().getObjectName(), file);
                    FileUtils.deleteQuietly(file);
                } else
                    success = false;
            }
            return success;
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, getOperation().getObjectName()));
        }
    }

    private boolean restoreVersionObjectMetadata() {
        try {
            boolean success = false;
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            for (Drive drive : getDriver().getDrivesAll()) {
                File file = drive.getObjectMetadataVersionFile(bucket, getOperation().getObjectName(), getOperation().getVersion());
                if (file.exists()) {
                    drive.putObjectMetadataFile(bucket, getOperation().getObjectName(), file);
                    FileUtils.deleteQuietly(file);
                    success=true;
                } 
            }
            return success;
        } catch (Exception e) {
            throw new InternalCriticalException(e, opInfo(getOperation()));
        }
    }

    
}
