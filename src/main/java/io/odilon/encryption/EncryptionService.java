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

import io.odilon.service.SystemService;

/**
 * 
 * @see {@link OdilonEncryptionService}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface EncryptionService extends SystemService {
	
	public static final String ENCRYPTION_ALGORITHM = "AES";
	public static final String ENCRYPTION_ALGORITHM_METHOD = "AES/GCM/NoPadding";
	public static final int IV_LENGTH_BIT = 128;
	
	public InputStream encryptStream(InputStream inputStream);
	public InputStream decryptStream(InputStream inputStream);

}
