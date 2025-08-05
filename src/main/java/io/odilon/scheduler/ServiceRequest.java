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

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Map;

import org.springframework.context.ApplicationContextAware;

/**
 * <p>
 * Jobs executed Async by the {@link SchedulerService}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface ServiceRequest extends Serializable, ApplicationContextAware {

    /** Unique id */
    public Serializable getId();

    public void setId(Serializable id);

    public void execute();

    public String getUUID();

    /**
     * [0.0 - 1.0] NOTE: not all Requests implement it
     */
    public double getProgress();

    public boolean isCronJob();

    public boolean isExecuting();

    public void setStart(OffsetDateTime start);

    public void setStatus(ServiceRequestStatus status);

    public void setEnd(OffsetDateTime end);

    public OffsetDateTime started();

    public OffsetDateTime ended();

    public String getName();

    public void setName(String name);

    public String getDescription();

    public void setDescription(String des);

    public void stop();

    public void setParameters(Map<String, String> map);

    public Map<String, String> getParameters();

    public void setExecuteAfter(OffsetDateTime d);

    public OffsetDateTime getExecuteAfter();

    public boolean isSuccess();

    public void setRetries(int retries);

    public int getRetries();

    public void setTimeZone(String timeZoneID);

    public String getTimeZone();

}
