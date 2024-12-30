package io.odilon.virtualFileSystem.raid6;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDSixRollbackDeleteHandler extends RAIDSixRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDSixRollbackDeleteHandler.class.getName());

    public RAIDSixRollbackDeleteHandler(RAIDSixDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {

        boolean done = false;

        try {
            
            // rollback is the same for both operations
            if (getOperation().getOperationCode() == OperationCode.DELETE_OBJECT)
                restoreMetadata();

            else if (getOperation().getOperationCode() == OperationCode.DELETE_OBJECT_PREVIOUS_VERSIONS)
                restoreMetadata();

            done = true;

        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e);
            else
                logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery())
                getOperation().cancel();
        }
    }

    /**
     * restore metadata directory
     */
    private void restoreMetadata() {
        
        ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
        String objectName = getOperation().getObjectName();
        
        for (Drive drive : getDriver().getDrivesAll()) {
            String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucket) + File.separator + objectName;
            String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucket, objectName);
            try {
                if ((new File(objectMetadataBackupDirPath)).exists())
                    FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
            } catch (IOException e) {
                throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
            }
        }
    }

}
