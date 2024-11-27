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

import java.time.OffsetDateTime;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;

/**
 * <p>
 * This is a Thread that executes a ServiceRequest
 * </p>
 * <p>
 * The Thread that executes the Request is one of the threads from the
 * Dispatcher's pool
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ServiceRequestExecutor implements Runnable {

    static private Logger logger = Logger.getLogger(ServiceRequestExecutor.class.getName());

    private ServiceRequest request;
    private SchedulerWorker schedulerWorker;

    private boolean success = false;

    public ServiceRequestExecutor(ServiceRequest rqt, SchedulerWorker schedulerWorker) {
        this.request = rqt;
        this.schedulerWorker = schedulerWorker;
    }

    @Override
    public void run() {
        try {

            this.request.setStart(OffsetDateTime.now());
            this.request.setStatus(ServiceRequestStatus.RUNNING);
            this.request.execute();
            this.success = this.request.isSuccess();

        } catch (Throwable e) {
            logger.error(e, SharedConstant.NOT_THROWN);
            this.request.setStatus(ServiceRequestStatus.ERROR);
            this.success = false;

        } finally {

            try {
                this.request.setEnd(OffsetDateTime.now());

                if (this.success)
                    this.schedulerWorker.close(this.request);
                else
                    this.schedulerWorker.fail(this.request);

            } catch (Throwable e) {
                logger.error(e, SharedConstant.NOT_THROWN);
                try {
                    this.request.setStatus(ServiceRequestStatus.ERROR);
                    this.schedulerWorker.fail(this.request);
                } catch (Exception e1) {
                    logger.error(e1, SharedConstant.NOT_THROWN);
                }
            }
        }
    }

    public void setSchedulerService(SchedulerWorker schedulerWorker) {
        this.schedulerWorker = schedulerWorker;
    }
}
