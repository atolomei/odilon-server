/*
 * Odilon Object Storage
 * (C) Novamens 
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

/**
 * <p>
 * Clean up process for Updates is still Sync with the transaction. <br/>
 * It is not used (as of version <b>0.9-beta</b>), Clean up is done Sync and as
 * part of the update transaction
 * </p>
 * 
 * @see {@link RAIDZeroUpdateObjectHandler}, {@link RAIDOneUpdateObjectHandler}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
@JsonTypeName("afterUpdateObject")
public class AfterUpdateObjectServiceRequest extends AbstractServiceRequest implements StandardServiceRequest {

    static private Logger logger = Logger.getLogger(AfterUpdateObjectServiceRequest.class.getName());

    private static final long serialVersionUID = 1L;

    @JsonProperty("bucketName")
    String bucketName;

    @JsonProperty("objectName")
    String objectName;

    @JsonIgnore
    private boolean isSuccess = false;

    protected AfterUpdateObjectServiceRequest() {
    }

    public AfterUpdateObjectServiceRequest(String bucketName, String objectName) {
        this.bucketName = bucketName;
        this.objectName = objectName;
    }

    @Override
    public String getUUID() {
        return ((this.bucketName != null) ? this.bucketName : "null") + ":"
                + ((this.objectName != null) ? this.objectName : "null");
    }

    @Override
    public boolean isObjectOperation() {
        return true;
    }

    @Override
    public boolean isSuccess() {
        return this.isSuccess;
    }

    /**
     * <p>
     * {@link ServiceRequestExecutor} closes the Request
     * </p>
     */
    @Override
    public void execute() {
        try {

            setStatus(ServiceRequestStatus.RUNNING);
            clean();
            this.isSuccess = true;
            setStatus(ServiceRequestStatus.COMPLETED);

        } catch (Exception e) {
            setStatus(ServiceRequestStatus.ERROR);
            this.isSuccess = false;
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    @Override
    public void stop() {
        this.isSuccess = true;
    }

    private void clean() {
        logger.debug(this.getClass().getName()
                + " -> not used yet. Clean up process for Updates is still Sync with the transaction");
    }

}
