package io.odilon.virtualFileSystem.raid0;

import java.io.File;
import java.io.IOException;

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

public class RAIDZeroRollbackUpdateHandler extends RAIDZeroRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroRollbackUpdateHandler.class.getName());

    public RAIDZeroRollbackUpdateHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    /**
     * <p>
     * The procedure is the same for Version Control
     * </p>
     * 
     */
    @Override
    protected void rollback() {

        if (getOperation() == null)
            return;

        switch (getOperation().getOperationCode()) {
        case UPDATE_OBJECT: {
            rollbackJournalUpdate(getOperation(), isRecovery());
            break;
        }
        case UPDATE_OBJECT_METADATA: {
            rollbackJournalUpdateMetadata(getOperation(), isRecovery());
            break;
        }
        case RESTORE_OBJECT_PREVIOUS_VERSION: {
            rollbackJournalUpdate(getOperation(), isRecovery());
            break;
        }
        default: {
            break;
        }
        }
    }

    /**
     * @param op
     * @param recoveryMode
     */
    private void rollbackJournalUpdate(VirtualFileSystemOperation op, boolean recoveryMode) {

        boolean done = false;
        try {
            if (isStandByEnabled())
                getReplicationService().cancel(op);
            ServerBucket bucket = getBucketCache().get(op.getBucketId());
            restoreVersionDataFile(bucket, op.getObjectName(), op.getVersion());
            restoreVersionMetadata(bucket, op.getObjectName(), op.getVersion());
            done = true;
        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(opInfo(op), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, "Rollback | " + getDriver().opInfo(op));
            else
                logger.error(opInfo(op), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                op.cancel();
            }
        }
    }

    /**
     * @param operation
     * @param recoveryMode
     */
    private void rollbackJournalUpdateMetadata(VirtualFileSystemOperation operation, boolean recoveryMode) {

        boolean done = false;
        try {
            ServerBucket bucket = getBucketCache().get(operation.getBucketId());

            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(operation);

            if (operation.getOperationCode() == OperationCode.UPDATE_OBJECT_METADATA)
                restoreMetadata(bucket, operation.getObjectName());

            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(e, getDriver().opInfo(operation), SharedConstant.NOT_THROWN);

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

    /**
     * @param bucketName
     * @param objectName
     * @param version
     */
    private boolean restoreVersionMetadata(ServerBucket bucket, String objectName, int versionToRestore) {
        try {
            Drive drive = getWriteDrive(bucket, objectName);
            File file = drive.getObjectMetadataVersionFile(bucket, objectName, versionToRestore);
            if (file.exists()) {
                drive.putObjectMetadataFile(bucket, objectName, file);
                FileUtils.deleteQuietly(file);
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    private boolean restoreVersionDataFile(ServerBucket bucket, String objectName, int version) {
        try {
            Drive drive = getWriteDrive(bucket, objectName);
            ObjectPath path = new ObjectPath(drive, bucket, objectName);
            File file = path.dataFileVersionPath(version).toFile();

            if (file.exists()) {
                ((SimpleDrive) drive).putObjectDataFile(bucket.getId(), objectName, file);
                FileUtils.deleteQuietly(file);
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    /**
     * restore metadata directory
     */
    private void restoreMetadata(ServerBucket bucket, String objectName) {
        String objectMetadataBackupDirPath = getDriver().getWriteDrive(bucket, objectName).getBucketWorkDirPath(bucket)
                + File.separator + objectName;
        String objectMetadataDirPath = getDriver().getWriteDrive(bucket, objectName).getObjectMetadataDirPath(bucket, objectName);
        try {
            FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
        } catch (IOException e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

}
