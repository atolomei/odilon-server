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
package io.odilon.virtualFileSystem.raid6;

/**
 * <p>
 * Lifecycle status of a {@link RAIDSixVolume}.
 * </p>
 *
 * <ul>
 *   <li><b>ACTIVE</b>  – accepts both reads and writes; this is where new objects are written.</li>
 *   <li><b>READONLY</b> – accepts reads only; write traffic is directed to another ACTIVE volume.</li>
 *   <li><b>ARCHIVED</b> – scheduled for removal; no I/O accepted once data has been migrated away.</li>
 * </ul>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public enum VolumeStatus {

    /** Accepts reads and writes.  New objects land here. */
    ACTIVE   ("active",   1),
    /** Accepts reads only.  A different volume is the write target. */
    READONLY ("readonly", 2),
    /** No I/O accepted.  Pending removal after data migration. */
    ARCHIVED ("archived", 3);

    private final String name;
    private final int    code;

    VolumeStatus(String name, int code) {
        this.name = name;
        this.code = code;
    }

    public String getName() { return name; }
    public int    getCode() { return code; }

    /**
     * Case-insensitive lookup by name, defaults to {@link #ACTIVE} if not found.
     */
    public static VolumeStatus fromName(String name) {
        if (name == null) return ACTIVE;
        for (VolumeStatus s : values()) {
            if (s.name.equalsIgnoreCase(name.trim())) return s;
        }
        return ACTIVE;
    }

    @Override
    public String toString() { return name; }
}
