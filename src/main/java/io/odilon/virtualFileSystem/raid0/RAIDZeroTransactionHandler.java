package io.odilon.virtualFileSystem.raid0;

import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public abstract class RAIDZeroTransactionHandler extends RAIDZeroHandler {

    public RAIDZeroTransactionHandler(RAIDZeroDriver driver) {
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
