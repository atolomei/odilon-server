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

    public RAIDOneRollbackUpdateHandler(RAIDOneDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {

        if (getOperation() == null)
            return;

        if (getOperation().getOperationCode() == OperationCode.UPDATE_OBJECT)
            rollbackJournalUpdate(getOperation(), isRecovery());

        else if (getOperation().getOperationCode() == OperationCode.UPDATE_OBJECT_METADATA)
            rollbackJournalUpdateMetadata(getOperation(), isRecovery());

        else if (getOperation().getOperationCode() == OperationCode.RESTORE_OBJECT_PREVIOUS_VERSION)
            rollbackJournalUpdate(getOperation(), isRecovery());
    }

    private void rollbackJournalUpdate(VirtualFileSystemOperation operation, boolean recoveryMode) {

        boolean done = false;

        try {

            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(operation);

            ServerBucket bucket = getBucketCache().get(operation.getBucketId());

            restoreVersionObjectDataFile(bucket, operation.getObjectName(), operation.getVersion());
            restoreVersionObjectMetadata(bucket, operation.getObjectName(), operation.getVersion());

            done = true;

        } catch (InternalCriticalException e) {
            logger.error(getDriver().opInfo(operation));
            if (!recoveryMode)
                throw (e);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, opInfo(operation));
            else
                logger.error(e, opInfo(operation), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                operation.cancel();
            }
        }
    }

    private boolean restoreVersionObjectDataFile(ServerBucket bucket, String objectName, int version) {
        try {
            boolean success = true;

            for (Drive drive : getDriver().getDrivesAll()) {
                ObjectPath path = new ObjectPath(drive, bucket, objectName);
                File file = path.dataFileVersionPath(version).toFile();
                // File file = ((SimpleDrive) drive).getObjectDataVersionFile(bucket.getId(),
                // objectName, version);

                if (file.exists()) {
                    ((SimpleDrive) drive).putObjectDataFile(bucket.getId(), objectName, file);
                    FileUtils.deleteQuietly(file);
                } else
                    success = false;
            }
            return success;
        } catch (Exception e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
        }
    }

    private boolean restoreVersionObjectMetadata(ServerBucket bucket, String objectName, int version) {
        try {

            boolean success = true;
            for (Drive drive : getDriver().getDrivesAll()) {
                File file = drive.getObjectMetadataVersionFile(bucket, objectName, version);
                if (file.exists()) {
                    drive.putObjectMetadataFile(bucket, objectName, file);
                    FileUtils.deleteQuietly(file);
                } else
                    success = false;
            }
            return success;
        } catch (Exception e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(bucket, objectName));
        }
    }

    private void rollbackJournalUpdateMetadata(VirtualFileSystemOperation operation, boolean recoveryMode) {

        boolean done = false;
        try {
            if (getVirtualFileSystemService().getServerSettings().isStandByEnabled())
                getVirtualFileSystemService().getReplicationService().cancel(operation);

            ServerBucket bucket = getBucketCache().get(operation.getBucketId());

            restoreVersionObjectMetadata(bucket, operation.getObjectName(), operation.getVersion());

            done = true;

        } catch (InternalCriticalException e) {
            logger.error(opInfo(operation));
            if (!recoveryMode)
                throw (e);

        } catch (Exception e) {

            if (!recoveryMode)
                throw new InternalCriticalException(e, opInfo(operation));
            else
                logger.error(opInfo(operation), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                operation.cancel();
            }
        }
    }

}
