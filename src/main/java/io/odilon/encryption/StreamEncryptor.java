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
package io.odilon.encryption;

import java.io.InputStream;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface StreamEncryptor {

    public EncryptedInputStream encrypt(InputStream inputStream, String key, String iv);

    public InputStream decrypt(InputStream inputStream, String encryptedKey, String iv);

    public String getNewKey();

    public String getIV();

    public StreamEncryptorInfo getStreamEncryptionInfo(String key, String ivStr);
}
