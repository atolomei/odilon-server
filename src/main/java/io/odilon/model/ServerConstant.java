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
package io.odilon.model;

/**
 * <p>
 * Global constants used by all classes
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ServerConstant {

    static public final String applicationName = "Odilon";
    
    static public final String SEPARATOR = "---------------------------------";

    static public final String MASTER_MODE = "master";
    static public final String STANDBY_MODE = "standby";

    static final public String ENC_SUFFIX = ".enc";

    static final public String BO_SEPARATOR = "$";

    static final public String JSON = ".json";

    /** default expiration for a presigned URL is 7 days in seconds */
    static final public int DEFAULT_EXPIRY_TIME = SharedConstant.DEFAULT_EXPIRY_TIME;
    static final public int DEFAULT_COMMANDS_PAGE_SIZE = 1000;
    static final public int DEFAULT_PAGE_SIZE = SharedConstant.DEFAULT_PAGE_SIZE;

    /** 15 minutes */
    static public final long MAX_CONNECTION_IDLE_TIME_SECS = 15 * 60;
    static public final int BUCKET_ITERATOR_DEFAULT_BUFFER_SIZE = 1000;
    static public final int TRAFFIC_TOKENS_DEFAULT = 12;
    static public final int DAYS_INTEGRITY_CHECKS = 180;

    static public final String DEFAULT_KEY_ALGORITHM = "AES";

    static public final String ENABLE_ENCRYPTION_SCRIPT_LINUX = "enable-encryption.sh";
    static public final String ENABLE_ENCRYPTION_SCRIPT_WINDOWS = "enable-encryption.bat";

    static public final String REKEY_ENCRYPTION_SCRIPT_LINUX = "rekey-encryption.sh";
    static public final String REKEY_ENCRYPTION_SCRIPT_WINDOWS = "rekey-encryption.bat";

    static public final double KB = 1024.0;
    static public final double MB = 1024.0 * KB;
    static public final double GB = 1024.0 * MB;

    static public final int iKB = 1024;
    static public final int iMB = 1024 * iKB;
    static public final int iGB = 1024 * iMB;

    static public final int BYTES_IN_INT = 4;
    static public final int BYTES_IN_LONG = 16;

    static public final int MAX_CHUNK_SIZE = 32 * iMB;

    public static final int BUFFER_SIZE = 8192;

}
