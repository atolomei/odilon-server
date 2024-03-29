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

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class JCipherStreamEncryptorInfo implements StreamEncryptorInfo {
    private String encryptionAlgorithm;
    private String encryptedKey;
    private String keyAlgorithm;
    private KeyEncryptor keyEncryptor;

    public JCipherStreamEncryptorInfo() {
    }

    public JCipherStreamEncryptorInfo(JCipherStreamEncryptor jCipherStreamEncryption, String encryptedKey) {
        this.setEncryptionAlgorithm(jCipherStreamEncryption.getEncryptionAlgorithm());
        this.setKeyAlgorithm(jCipherStreamEncryption.getKeyAlgorithm());
        this.setKeyEncryptor(jCipherStreamEncryption.getKeyEncryptor());
        this.setEncryptedKey(encryptedKey);
    }

    @Override
    public StreamEncryptor getStreamEncryption() {
        return new JCipherStreamEncryptor(encryptionAlgorithm, keyAlgorithm, keyEncryptor);
    }

    public String getEncryptionAlgorithm() {
        return encryptionAlgorithm;
    }

    public void setEncryptionAlgorithm(String encryptionAlgorithm) {
        this.encryptionAlgorithm = encryptionAlgorithm;
    }

    @Override
    public String getEncryptedKey() {
        return encryptedKey;
    }

    public void setEncryptedKey(String encryptedKey) {
        this.encryptedKey = encryptedKey;
    }

    public String getKeyAlgorithm() {
        return keyAlgorithm;
    }

    public void setKeyAlgorithm(String keyAlgorithm) {
        this.keyAlgorithm = keyAlgorithm;
    }

    public KeyEncryptor getKeyEncryptor() {
        return keyEncryptor;
    }

    public void setKeyEncryptor(KeyEncryptor keyEncryptor) {
        this.keyEncryptor = keyEncryptor;
    }
}
