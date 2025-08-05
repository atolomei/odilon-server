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
package io.odilon.virtualFileSystem;

import java.io.Serializable;
import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.odilon.model.BaseObject;
import io.odilon.virtualFileSystem.model.DriveStatus;

/**
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@JsonInclude(Include.NON_NULL)
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

    @JsonProperty("raidSetup")
    private String raidSetup;

    @JsonProperty("raidDrives")
    private int raidDrives;

    public DriveInfo() {
    }

    public DriveInfo(String name, String driveId, OffsetDateTime dateConnected, DriveStatus status, int configOrder, String raidSetup, int raidDrives) {
        this.name = name;
        this.driveId = driveId;
        this.dateConnected = dateConnected;
        this.driveStatus = status;
        this.order = configOrder;
        this.raidSetup=raidSetup;
        this.raidDrives=raidDrives;
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

      public String getRaidSetup() {
        return raidSetup;
    }

    public void setRaidSetup(String raidSetup) {
        this.raidSetup = raidSetup;
    }

    public int getRaidDrives() {
        return raidDrives;
    }

    public void setRaidDrives(int raidDrives) {
        this.raidDrives = raidDrives;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(this.getClass().getSimpleName());
        str.append(toJSON());
        return str.toString();
    }
}
