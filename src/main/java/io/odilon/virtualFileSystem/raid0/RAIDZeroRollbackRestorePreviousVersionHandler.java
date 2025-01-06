package io.odilon.virtualFileSystem.raid0;

import java.io.File;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDZeroRollbackRestorePreviousVersionHandler extends RAIDZeroRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroRollbackRestorePreviousVersionHandler.class.getName());

    public RAIDZeroRollbackRestorePreviousVersionHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation,
            boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {

        boolean done = false;
        try {
            
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            Drive drive = getWriteDrive(bucket, getOperation().getObjectName());
            ObjectPath path = new ObjectPath(drive, bucket, getOperation().getObjectName());

            File file = path.dataFileVersionPath(getOperation().getVersion()).toFile();
            if (file.exists()) {
                ((SimpleDrive) drive).putObjectDataFile(bucket.getId(), getOperation().getObjectName(), file);
                FileUtils.deleteQuietly(file);
            }

            File file2 = drive.getObjectMetadataVersionFile(bucket, getOperation().getObjectName(), getOperation().getVersion());
            if (file2.exists()) {
                drive.putObjectMetadataFile(bucket, getOperation().getObjectName(), file2);
                FileUtils.deleteQuietly(file2);
            }

            done = true;

        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(info(), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, info());
            else
                logger.error(info(), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery()) {
                getOperation().cancel();
            }
        }
    }

}
