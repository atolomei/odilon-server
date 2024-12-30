package io.odilon.virtualFileSystem.raid0;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDZeroRollbackDeleteHandler extends RAIDZeroRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroRollbackDeleteHandler.class.getName());

    public RAIDZeroRollbackDeleteHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {

        if (getOperation() == null)
            return;

        boolean done = false;

        ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());

        try {

            if (isStandByEnabled())
                getReplicationService().cancel(getOperation());

            // Rollback is the same for both operations -> DELETE_OBJECT and
            // DELETE_OBJECT_PREVIOUS_VERSIONS
            if (getOperation().getOperationCode() == OperationCode.DELETE_OBJECT)
                restoreMetadata(bucket, getOperation().getObjectName());

            else if (getOperation().getOperationCode() == OperationCode.DELETE_OBJECT_PREVIOUS_VERSIONS)
                restoreMetadata(bucket, getOperation().getObjectName());

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
                logger.error(opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery())
                getOperation().cancel();
        }

    }

    /**
     * 
     * restore metadata directory
     * 
     * @param bucketName
     * @param objectName
     */
    private void restoreMetadata(ServerBucket bucket, String objectName) {

        String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, objectName).getBucketWorkDirPath(bucket)
                + File.separator + objectName;
        String objectMetadataDirPath = getDriver().getWriteDrive(bucket, objectName).getObjectMetadataDirPath(bucket, objectName);
        try {
            FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
        } catch (InternalCriticalException e) {
            throw e;
        } catch (IOException e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

}
