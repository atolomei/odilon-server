package io.odilon.virtualFileSystem.raid6;

import java.io.File;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDSixRollbackCreateHandler extends RAIDSixRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDSixRollbackCreateHandler.class.getName());

    public RAIDSixRollbackCreateHandler(RAIDSixDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {
        
        if (getOperation() == null)
            return;


        String objectName = getOperation().getObjectName();

        ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());

        boolean done = false;

        try {
            if (getServerSettings().isStandByEnabled())
                getReplicationService().cancel(getOperation());

            ObjectMetadata meta = null;

            // remove metadata dir on all drives
            for (Drive drive : getDriver().getDrivesAll()) {
                File f_meta = drive.getObjectMetadataFile(bucket, objectName);
                if ((meta == null) && (f_meta != null)) {
                    try {
                        meta = drive.getObjectMetadata(bucket, objectName);
                    } catch (Exception e) {
                        logger.warn("can not load meta -> d: " + drive.getName() + SharedConstant.NOT_THROWN);
                    }
                }
                FileUtils.deleteQuietly(new File(drive.getObjectMetadataDirPath(bucket, objectName)));
            }

            /// remove data dir on all drives
            if (meta != null)
                getDriver().getObjectDataFiles(meta, bucket, Optional.empty()).forEach(file -> {
                    FileUtils.deleteQuietly(file);
                });

            done = true;

        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(e, getOperation().toString() + SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, "Rollback: " + getOperation().toString() + SharedConstant.NOT_THROWN);
            else
                logger.error(e, getOperation().toString() + SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery()) {
                getOperation().cancel();
            }
        }
    }
}
