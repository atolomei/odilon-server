/*
 * Odilon Object Storage
 * (c) kbee 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.odilon.scheduler;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.replication.ReplicationService;
import io.odilon.virtualFileSystem.OdilonVirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * tServiceRequest must be {@link Serializable}<br/>
 * It is executed by a Thread ({@link ServiceRequestExecutor}) of the Scheduler
 * thread pool
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@Component
@Scope("prototype")
@JsonTypeName("standByReplica")
public class StandByReplicaServiceRequest extends AbstractServiceRequest {

    static private Logger logger = Logger.getLogger(StandByReplicaServiceRequest.class.getName());

    private static final long serialVersionUID = 1L;

    @JsonIgnore
    private boolean isSuccess = false;

    @JsonProperty("operation")
    private OdilonVirtualFileSystemOperation operation;

    protected StandByReplicaServiceRequest() {
    }

    public StandByReplicaServiceRequest(VirtualFileSystemOperation operation) {
        this.operation = (OdilonVirtualFileSystemOperation) operation;
    }

    /**
     * <p>
     * {@link ServiceRequestExecutor} will close/fail/cancel he request after this
     * method
     * </p>
     */
    @Override
    public void execute() {

        try {
            setStatus(ServiceRequestStatus.RUNNING);
            ReplicationService rs = getApplicationContext().getBean(ReplicationService.class);
            if (rs.isStandByEnabled()) {
                rs.replicate(getVFSOperation());
                isSuccess = true;
            } else
                isSuccess = true;
            setStatus(ServiceRequestStatus.COMPLETED);

        } catch (Exception e) {
            isSuccess = false;
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    @Override
    public void stop() {
        this.isSuccess = true;
    }

    @Override
    public String getUUID() {
        return getVFSOperation().getUUID();
    }

    public VirtualFileSystemOperation getVFSOperation() {
        return operation;
    }

    @Override
    public boolean isSuccess() {
        return isSuccess;
    }

}
