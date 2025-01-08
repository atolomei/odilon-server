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
package io.odilon.virtualFileSystem.raid6;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.model.BaseObject;

/**
 * <p>
 * List of encoded blocks stored in File System See the coding convention:
 * {@link RAIDSixDriver}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixBlocks extends BaseObject {

    @JsonIgnore
    private List<File> encodedBlocks = new ArrayList<File>();


    @JsonProperty("fileSize")
    private long fileSize;

    public RAIDSixBlocks() {
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public List<File> getEncodedBlocks() {
        return encodedBlocks;
    }

    public void setEncodedBlocks(List<File> encodedBlocks) {
        this.encodedBlocks = encodedBlocks;
    }

    @Override
    public String toJSON() {
        StringBuilder str = new StringBuilder();
        if (getEncodedBlocks() != null) {
            str.append("[");
            getEncodedBlocks().forEach(f -> str.append("\"" + f.getName() + "\" "));
            str.append("]");
        }
        str.append("\"fileSize\":" + String.valueOf(getFileSize()));
        return str.toString();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(this.getClass().getSimpleName());
        str.append(toJSON());
        return str.toString();
    }
}
