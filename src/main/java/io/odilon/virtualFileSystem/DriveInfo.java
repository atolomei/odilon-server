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
package io.odilon.virtualFileSystem;

import java.io.Serializable;
import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.model.BaseObject;
import io.odilon.virtualFileSystem.model.DriveStatus;

/**
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class DriveInfo extends BaseObject implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("name")
    private String name;

    @JsonProperty("driveId")
    private String driveId;

    @JsonProperty("dateConnected")
    private OffsetDateTime dateConnected;

    @JsonProperty("status")
    private DriveStatus driveStatus;

    @JsonProperty("order")
    private int order;

    public DriveInfo() {
    }

    public DriveInfo(String name, String driveId, OffsetDateTime dateConnected, DriveStatus status, int configOrder) {
        this.name = name;
        this.driveId = driveId;
        this.dateConnected = dateConnected;
        this.driveStatus = status;
        this.order = configOrder;
    }

    public DriveStatus getStatus() {
        return this.driveStatus;
    }

    public void setStatus(DriveStatus status) {
        this.driveStatus = status;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public String getDriveId() {
        return driveId;
    }

    public void setDriveId(String driveId) {
        this.driveId = driveId;
    }

    public OffsetDateTime getDateConnected() {
        return dateConnected;
    }

    public void setDateConnected(OffsetDateTime dateConnected) {
        this.dateConnected = dateConnected;
    }

}
