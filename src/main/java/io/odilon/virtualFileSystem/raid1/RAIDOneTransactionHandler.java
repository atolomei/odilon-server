package io.odilon.virtualFileSystem.raid1;

import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDOneTransactionHandler extends RAIDOneHandler {

    public RAIDOneTransactionHandler(RAIDOneDriver driver) {
        super(driver);
    }

    protected void rollback(VirtualFileSystemOperation operation) {
        getDriver().rollback(operation, false);
    }

    protected void rollback(VirtualFileSystemOperation operation, boolean recoveryMode) {
        getDriver().rollback(operation, recoveryMode);
    }

    protected void rollback(VirtualFileSystemOperation operation, Object payload, boolean recoveryMode) {
        getDriver().rollback(operation, payload, recoveryMode);
    }

}
