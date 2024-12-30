package io.odilon.virtualFileSystem.raid1;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDOneRollbackDeleteHandler extends RAIDOneRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDOneRollbackDeleteHandler.class.getName());
    
    public RAIDOneRollbackDeleteHandler(RAIDOneDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {
        
        if (getOperation() == null)
            return;
        
        String objectName = getOperation().getObjectName();
        Long bucketId = getOperation().getBucketId();

        Check.requireNonNullArgument(bucketId, "bucketId is null");
        Check.requireNonNullArgument(objectName, "objectName is null or empty | b:" + bucketId.toString());

        boolean done = false;

        try {

            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(getOperation());

            ServerBucket bucket = getCacheBucket(getOperation().getBucketId());

            /** rollback is the same for both operations */
            if (getOperation().getOperationCode() == OperationCode.DELETE_OBJECT)
                restoreMetadata(bucket, objectName);

            else if (getOperation().getOperationCode() == OperationCode.DELETE_OBJECT_PREVIOUS_VERSIONS)
                restoreMetadata(bucket, objectName);

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
    
    private void restoreMetadata(ServerBucket bucket, String objectName) {
        /** restore metadata directory */
        for (Drive drive : getDriver().getDrivesAll()) {
            String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucket) + File.separator + objectName;
            String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucket, objectName);
            try {
                FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
            } catch (IOException e) {
                throw new InternalCriticalException(e, objectInfo(bucket, objectName));
            }
        }
    }

}
