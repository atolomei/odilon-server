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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * 
 * <p>
 * Service that encrypts/decrypts Object's encryption key (if encryption is
 * enabled in odilon.properties)
 * </p>
 * 
 * <p>
 * Every object is encrypted with its own key. The key in turn is encrypted by a
 * KeyEncryptor. One of these two is used:
 * </p>
 * 
 * <br/>
 * <ul>
 * <li>{@link OdilonKeyEncryptor} is Odilon's key encryption service</li>
 * 
 * <br/>
 * <li>{@link VaultKeyEncryptor} is the key encryption service based in Vault
 * KMS, it is used if Vault is enabled in odilon.properties.</li>
 * </ul>
 * 
 * @see {@link MasterKeyService}
 * @see {@link EncryptionService}
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE)
public interface KeyEncryptor {

    byte[] encryptKey(byte[] key, byte[] iv);

    byte[] decryptKey(byte[] key, byte[] iv);

}
