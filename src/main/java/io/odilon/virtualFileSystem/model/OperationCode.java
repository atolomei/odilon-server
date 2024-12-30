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
package io.odilon.virtualFileSystem.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
//import java.util.ResourceBundle;

/**
 * <p>
 * Atomic operations that managed by the {@link JournalService}. Include: Bucket
 * CRUD, Object CRUD and versions, Sync drive, server metadata, server Masterkey
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public enum OperationCode {

    CREATE_BUCKET("create_bucket", 0, "b", true), // 1
    UPDATE_BUCKET("update_bucket", 1, "b", true), // 2
    DELETE_BUCKET("delete_bucket", 2, "b", true), // 3

    CREATE_OBJECT("create_object", 10, "o", true), // 4
    UPDATE_OBJECT("update_object", 21, "o", true), // 5
    UPDATE_OBJECT_METADATA("update_object_metadata", 22, "o", true), // 6
    DELETE_OBJECT("delete_object", 23, "o", true), // 7
    DELETE_OBJECT_PREVIOUS_VERSIONS("delete_object_previous_versions", 24, "o", true), // 8
    RESTORE_OBJECT_PREVIOUS_VERSION("restore_object_previous_versions", 25, "o", true), // 9

    SYNC_OBJECT_NEW_DRIVE("sync_object", 26, "o", false), // 10

    CREATE_SERVER_METADATA("create_server_metadata", 70, "s", false), // 11
    UPDATE_SERVER_METADATA("update_server_metadata", 88, "s", false), // 12

    CREATE_SERVER_MASTERKEY("create_server_key", 90, "s", false), // 13
    INTEGRITY_CHECK("inttegrity_check", 101, "o", false); // 14

    static List<OperationCode> ops;

    private String name;
    private int code;
    private String enttiyGroupCode;
    private boolean replicates;

    public static OperationCode fromId(String id) {

        if (id == null)
            throw new IllegalArgumentException("id is null");

        try {
            return get(Integer.valueOf(id).intValue());

        } catch (IllegalArgumentException e) {
            throw (e);
        } catch (Exception e) {
            throw new IllegalArgumentException("id not integer -> " + id);
        }
    }

    public static List<OperationCode> getValues() {

        if (ops != null)
            return ops;

        ops = new ArrayList<OperationCode>();

        ops.add(CREATE_BUCKET);
        ops.add(UPDATE_BUCKET);
        ops.add(DELETE_BUCKET);

        ops.add(CREATE_OBJECT);
        ops.add(UPDATE_OBJECT);
        ops.add(UPDATE_OBJECT_METADATA);

        ops.add(DELETE_OBJECT);
        ops.add(SYNC_OBJECT_NEW_DRIVE);

        ops.add(DELETE_OBJECT_PREVIOUS_VERSIONS);
        ops.add(RESTORE_OBJECT_PREVIOUS_VERSION);

        ops.add(CREATE_SERVER_METADATA);
        ops.add(UPDATE_SERVER_METADATA);

        ops.add(CREATE_SERVER_MASTERKEY);
        ops.add(INTEGRITY_CHECK);

        return ops;
    }

    /**
     * @param name
     * @return
     */
    public static OperationCode get(String name) {

        if (name == null)
            throw new IllegalArgumentException("name is null");

        String normalized = name.toUpperCase().trim();

        if (normalized.equals(CREATE_BUCKET.getName()))
            return CREATE_BUCKET;
        if (normalized.equals(UPDATE_BUCKET.getName()))
            return UPDATE_BUCKET;
        if (normalized.equals(DELETE_BUCKET.getName()))
            return DELETE_BUCKET;

        if (normalized.equals(CREATE_OBJECT.getName()))
            return CREATE_OBJECT;
        if (normalized.equals(UPDATE_OBJECT.getName()))
            return UPDATE_OBJECT;
        if (normalized.equals(DELETE_OBJECT.getName()))
            return DELETE_OBJECT;
        if (normalized.equals(SYNC_OBJECT_NEW_DRIVE.getName()))
            return SYNC_OBJECT_NEW_DRIVE;
        if (normalized.equals(UPDATE_OBJECT_METADATA.getName()))
            return UPDATE_OBJECT_METADATA;

        if (normalized.equals(DELETE_OBJECT_PREVIOUS_VERSIONS.getName()))
            return DELETE_OBJECT_PREVIOUS_VERSIONS;
        if (normalized.equals(RESTORE_OBJECT_PREVIOUS_VERSION.getName()))
            return RESTORE_OBJECT_PREVIOUS_VERSION;

        if (normalized.equals(CREATE_SERVER_METADATA.getName()))
            return CREATE_SERVER_METADATA;
        if (normalized.equals(UPDATE_SERVER_METADATA.getName()))
            return UPDATE_SERVER_METADATA;

        if (normalized.equals(CREATE_SERVER_MASTERKEY.getName()))
            return CREATE_SERVER_MASTERKEY;

        if (normalized.equals(INTEGRITY_CHECK.getName()))
            return INTEGRITY_CHECK;

        throw new IllegalArgumentException("unsuported name -> " + name);
    }

    public static OperationCode get(int code) {

        if (code == CREATE_BUCKET.getCode())
            return CREATE_BUCKET;
        if (code == UPDATE_BUCKET.getCode())
            return UPDATE_BUCKET;
        if (code == DELETE_BUCKET.getCode())
            return DELETE_BUCKET;

        if (code == CREATE_OBJECT.getCode())
            return CREATE_OBJECT;
        if (code == UPDATE_OBJECT.getCode())
            return UPDATE_OBJECT;
        if (code == DELETE_OBJECT.getCode())
            return DELETE_OBJECT;

        if (code == UPDATE_OBJECT_METADATA.getCode())
            return UPDATE_OBJECT_METADATA;
        if (code == DELETE_OBJECT_PREVIOUS_VERSIONS.getCode())
            return DELETE_OBJECT_PREVIOUS_VERSIONS;
        if (code == RESTORE_OBJECT_PREVIOUS_VERSION.getCode())
            return RESTORE_OBJECT_PREVIOUS_VERSION;
        if (code == SYNC_OBJECT_NEW_DRIVE.getCode())
            return SYNC_OBJECT_NEW_DRIVE;

        if (code == CREATE_SERVER_METADATA.getCode())
            return CREATE_SERVER_METADATA;
        if (code == UPDATE_SERVER_METADATA.getCode())
            return UPDATE_SERVER_METADATA;
        if (code == CREATE_SERVER_MASTERKEY.getCode())
            return CREATE_SERVER_MASTERKEY;

        if (code == INTEGRITY_CHECK.getCode())
            return INTEGRITY_CHECK;

        throw new IllegalArgumentException("unsuported code -> " + String.valueOf(code));
    }

    public String getDescription() {
        return getDescription(Locale.getDefault());
    }

    public String getDescription(Locale locale) {
        // ResourceBundle res =
        // ResourceBundle.getBundle(this.getClass().getSimpleName(), locale);
        // return res.getString(this.getName());
        return this.getName();
    }

    public String toJSON() {
        StringBuilder str = new StringBuilder();
        str.append("\"name\": \"" + name + "\"");
        str.append(", \"code\": " + code);
        str.append(", \"description\": \"" + getDescription() + "\"");
        return str.toString();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(this.getClass().getSimpleName() + "{");
        str.append(toJSON());
        str.append("}");
        return str.toString();
    }

    public String getName() {
        return name;
    }

    public int getCode() {
        return code;
    }

    public boolean isObjectOperation() {
        return (CREATE_OBJECT.equals(this) || UPDATE_OBJECT.equals(this) || DELETE_OBJECT.equals(this)
                || SYNC_OBJECT_NEW_DRIVE.equals(this) || UPDATE_OBJECT_METADATA.equals(this)) || INTEGRITY_CHECK.equals(this);
    }

    public String getEntityGroupCode() {
        return enttiyGroupCode;
    }

    private OperationCode(String name, int code, String groupCode, boolean replicates) {
        this.name = name;
        this.code = code;
        this.enttiyGroupCode = groupCode;
        this.replicates = replicates;
    }

    public boolean isReplicates() {
        return this.replicates;
    }
}
