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

        if (isStandByEnabled())
            getReplicationService().cancel(getOperation());

        switch (getOperation().getOperationCode()) {
        case UPDATE_OBJECT: {
            rollbackUpdate();
            break;
        }
        case UPDATE_OBJECT_METADATA: {
            rollbackUpdateMetadata();
            break;
        }
        case RESTORE_OBJECT_PREVIOUS_VERSION: {
            rollbackUpdate();
            break;
        }
        default: {
            throw new IllegalArgumentException(" not supported -> " + opInfo(getOperation()));
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
            else
                logger.error(opInfo(getOperation()), SharedConstant.NOT_THROWN);

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

    private void rollbackUpdate() {
        boolean done = false;
        try {
            //ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            //ObjectMetadata meta = getDriver().getObjectMetadataReadDrive(bucket, getOperation().getObjectName()).getObjectMetadata(bucket,
            //        getOperation().getObjectName());

            //if (meta != null) {
            restoreVersionObjectDataFile();
            restoreVersionObjectMetadata();
            //}
            done = true;

        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);

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

    private boolean restoreVersionObjectMetadata() {
        try {
            boolean success = true;
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            ObjectMetadata versionMeta = getDriver().getObjectMetadataVersion(bucket, getOperation().getObjectName(), getOperation().getVersion());
            for (Drive drive : getDriver().getDrivesAll()) {
                versionMeta.setDrive(drive.getName());
                drive.saveObjectMetadata(versionMeta);
            }
            return success;

        } catch (InternalCriticalException e) {
            throw e;
        } catch (Exception e) {
            throw new InternalCriticalException(e, opInfo(getOperation()));
        }
    }

    private boolean restoreVersionObjectDataFile() {
        try {
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            ObjectMetadata meta = getDriver().getObjectMetadataVersion(bucket, getOperation().getObjectName(), getOperation().getVersion());
            Map<Drive, List<String>> versionToRestore = getDriver().getObjectDataFilesNames(meta, Optional.of(getOperation().getVersion()));
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
                        throw new InternalCriticalException(e, opInfo(getOperation()));
                    }
                }

            }
            return true;

        } catch (InternalCriticalException e) {
            throw e;

        } catch (Exception e) {
            throw new InternalCriticalException(e, opInfo(getOperation()));
        }
    }

}
