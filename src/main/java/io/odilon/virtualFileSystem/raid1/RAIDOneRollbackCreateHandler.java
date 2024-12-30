package io.odilon.virtualFileSystem.raid1;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDOneRollbackCreateHandler extends RAIDOneRollbackHandler {

    private static Logger logger = Logger.getLogger(RAIDOneRollbackCreateHandler.class.getName());

    public RAIDOneRollbackCreateHandler(RAIDOneDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }

    @Override
    protected void rollback() {

        boolean done = false;

        try {
            
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            
            for (Drive drive : getDriver().getDrivesAll()) {
                drive.deleteObjectMetadata(bucket, getOperation().getObjectName());
                ObjectPath path = new ObjectPath(drive, bucket, getOperation().getObjectName());
                FileUtils.deleteQuietly(path.dataFilePath().toFile());
            }
            done = true;

        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(e, getDriver().opInfo(getOperation()), SharedConstant.NOT_THROWN);

        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, getDriver().opInfo(getOperation()));
            else
                logger.error(e, getDriver().opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } finally {
            if (done || isRecovery())
                getOperation().cancel();
        }
    }
}
