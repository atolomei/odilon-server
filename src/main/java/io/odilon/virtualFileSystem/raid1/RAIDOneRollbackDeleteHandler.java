package io.odilon.virtualFileSystem.raid1;


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

public class RAIDOneRollbackDeleteHandler extends RAIDOneRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDOneRollbackDeleteHandler.class.getName());

    public RAIDOneRollbackDeleteHandler(RAIDOneDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {

        boolean done = false;

        try {

            //if (getServerSettings().isStandByEnabled())
            //    getReplicationService().cancel(getOperation());

            /** rollback is the same for both operations */
            if (getOperation().getOperationCode() == OperationCode.DELETE_OBJECT)
                restoreMetadata();

            else if (getOperation().getOperationCode() == OperationCode.DELETE_OBJECT_PREVIOUS_VERSIONS)
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
                logger.error(opInfo(getOperation()), SharedConstant.NOT_THROWN);

        } finally {
            if (done || isRecovery())
                getOperation().cancel();
        }
    }

    private void restoreMetadata() {
        
        ServerBucket bucket = getCacheBucket(getOperation().getBucketId());
        
        /** restore metadata directory */
        for (Drive drive : getDriver().getDrivesAll()) {
            String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucket) + File.separator +  getOperation().getObjectName();
            String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucket,  getOperation().getObjectName());
            try {
                FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
            } catch (IOException e) {
                throw new InternalCriticalException(e, objectInfo(bucket,  getOperation().getObjectName()));
            }
        }
    }

}
