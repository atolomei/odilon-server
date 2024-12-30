package io.odilon.virtualFileSystem.raid6;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;

import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

public class RAIDSixRollbackUpdateHandler extends RAIDSixRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDSixRollbackUpdateHandler.class.getName());

    public RAIDSixRollbackUpdateHandler(RAIDSixDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {

        if (getOperation() == null)
            return;

        switch (getOperation().getOperationCode()) {
        case UPDATE_OBJECT: {
            rollbackJournalUpdate(getOperation(), getBucketCache().get(getOperation().getBucketId()), isRecovery());
            break;
        }
        case UPDATE_OBJECT_METADATA: {

            rollbackJournalUpdateMetadata(getOperation(), getBucketCache().get(getOperation().getBucketId()), isRecovery());
            break;
        }
        case RESTORE_OBJECT_PREVIOUS_VERSION: {
            rollbackJournalUpdate(getOperation(), getBucketCache().get(getOperation().getBucketId()), isRecovery());
            break;
        }
        default: {
            throw new IllegalArgumentException(
                    VirtualFileSystemOperation.class.getSimpleName() + " not supported ->  op: " + opInfo(getOperation()));
        }
        }
    }

    private void rollbackJournalUpdateMetadata(VirtualFileSystemOperation operation, ServerBucket bucket, boolean recoveryMode) {

        boolean done = false;

        try {
            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(operation);

            restoreVersionObjectMetadata(bucket, operation.getObjectName(), operation.getVersion());

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
                logger.error(opInfo(operation), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                operation.cancel();
            }
        }
    }

    private void rollbackJournalUpdate(VirtualFileSystemOperation op, ServerBucket bucket, boolean recoveryMode) {
        boolean done = false;
        try {
            if (isStandByEnabled())
                getReplicationService().cancel(op);

            ObjectMetadata meta = getDriver().getObjectMetadataReadDrive(bucket, op.getObjectName()).getObjectMetadata(bucket,
                    op.getObjectName());

            if (meta != null) {
                restoreVersionObjectDataFile(meta, bucket, op.getVersion());
                restoreVersionObjectMetadata(bucket, op.getObjectName(), op.getVersion());
            }

            done = true;

        } catch (InternalCriticalException e) {
            if (!recoveryMode)
                throw (e);
            else
                logger.error(e, opInfo(op), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!recoveryMode)
                throw new InternalCriticalException(e, opInfo(op));
            else
                logger.error(e, opInfo(op), SharedConstant.NOT_THROWN);
        } finally {
            if (done || recoveryMode) {
                op.cancel();
            }
        }
    }

    private boolean restoreVersionObjectMetadata(ServerBucket bucket, String objectName, int version) {
        try {
            boolean success = true;
            ObjectMetadata versionMeta = getDriver().getObjectMetadataVersion(bucket, objectName, version);
            for (Drive drive : getDriver().getDrivesAll()) {
                versionMeta.setDrive(drive.getName());
                drive.saveObjectMetadata(versionMeta);
            }
            return success;

        } catch (InternalCriticalException e) {
            throw e;
        } catch (Exception e) {
            throw new InternalCriticalException(e, objectInfo(bucket, objectName));
        }
    }

    private boolean restoreVersionObjectDataFile(ObjectMetadata meta, ServerBucket bucket, int version) {
        try {
            Map<Drive, List<String>> versionToRestore = getDriver().getObjectDataFilesNames(meta, Optional.of(version));
            for (Drive drive : versionToRestore.keySet()) {
                for (String name : versionToRestore.get(drive)) {
                    String arr[] = name.split(".v");
                    String headFileName = arr[0];
                    try {
                        if (new File(
                                drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR,
                                name).exists()) {
                            Files.copy(
                                    (new File(drive.getBucketObjectDataDirPath(bucket) + File.separator
                                            + VirtualFileSystemService.VERSION_DIR, name)).toPath(),
                                    (new File(drive.getBucketObjectDataDirPath(bucket), headFileName)).toPath(),
                                    StandardCopyOption.REPLACE_EXISTING);
                        }
                    } catch (IOException e) {
                        throw new InternalCriticalException(e, objectInfo(meta));
                    }
                }

            }
            return true;

        } catch (InternalCriticalException e) {
            throw e;

        } catch (Exception e) {
            throw new InternalCriticalException(e, getDriver().objectInfo(meta));
        }
    }

}
