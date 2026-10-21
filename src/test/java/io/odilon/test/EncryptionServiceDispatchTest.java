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
package io.odilon.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.dropwizard.metrics5.Meter;
import io.odilon.encryption.EncryptionService;
import io.odilon.encryption.KeyEncriptorWrapper;
import io.odilon.encryption.OdilonEncryptionService;
import io.odilon.encryption.v2.V2Format;
import io.odilon.errors.InternalCriticalException;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.service.ServerSettings;

/**
 * <p>
 * Unit test for the format dispatch in {@link OdilonEncryptionService}
 * (legacy / v2 / unknown), without a Spring context. The {@code KeyEncryptor}
 * is stubbed with an identity wrapper so no master key is required.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class EncryptionServiceDispatchTest {

    private OdilonEncryptionService service;

    @BeforeEach
    public void setup() {
        ServerSettings settings = mock(ServerSettings.class);
        when(settings.getEncryptionRecordSize()).thenReturn(V2Format.MIN_RECORD_SIZE);

        SystemMonitorService monitor = mock(SystemMonitorService.class);
        when(monitor.getEncrpytFileMeter()).thenReturn(new Meter());
        when(monitor.getDecryptFileMeter()).thenReturn(new Meter());

        KeyEncriptorWrapper keyEncryptor = mock(KeyEncriptorWrapper.class);
        when(keyEncryptor.encryptKey(any(byte[].class), any(byte[].class))).thenAnswer(inv -> ((byte[]) inv.getArgument(0)).clone());
        when(keyEncryptor.decryptKey(any(byte[].class), any(byte[].class))).thenAnswer(inv -> ((byte[]) inv.getArgument(0)).clone());

        this.service = new OdilonEncryptionService(settings, monitor, keyEncryptor);
    }

    @Test
    public void testV2RoundTripThroughService() throws Exception {
        byte[] plain = "v2 record-based AES-GCM-SIV data".getBytes();

        byte[] cipher;
        try (InputStream enc = this.service.encryptStreamV2(new ByteArrayInputStream(plain)).getInputStream()) {
            cipher = enc.readAllBytes();
        }
        try (InputStream dec = this.service.decryptStream(new ByteArrayInputStream(cipher), Integer.valueOf(EncryptionService.ENCRYPTION_FORMAT_V2))) {
            assertArrayEquals(plain, dec.readAllBytes());
        }
    }

    /**
     * NOTE: the legacy (null format) decrypt round trip is not testable without
     * a Spring context: the legacy JSON header reconstructs a
     * {@code KeyEncriptorWrapper} that resolves its delegate from the
     * application context. It is covered by {@code EncryptionAtRestTest}
     * (integration test on a configured server).
     */

    @Test
    public void testUnknownFormatFailsExplicitly() {
        assertThrows(InternalCriticalException.class,
                () -> this.service.decryptStream(new ByteArrayInputStream(new byte[] { 1, 2, 3 }), Integer.valueOf(99)));
    }

    @Test
    public void testV2StreamIsNotDecryptableAsLegacy() throws Exception {
        byte[] cipher;
        try (InputStream enc = this.service.encryptStreamV2(new ByteArrayInputStream("data".getBytes())).getInputStream()) {
            cipher = enc.readAllBytes();
        }
        /** dispatching a v2 stream to the legacy parser must fail, not return garbage */
        assertThrows(Exception.class, () -> {
            try (InputStream dec = this.service.decryptStream(new ByteArrayInputStream(cipher), null)) {
                dec.readAllBytes();
            }
        });
    }

    @Test
    public void testLegacyStreamIsNotDecryptableAsV2() throws Exception {
        byte[] cipher;
        try (InputStream enc = this.service.encryptStream(new ByteArrayInputStream("data".getBytes())).getInputStream()) {
            cipher = enc.readAllBytes();
        }
        assertThrows(Exception.class, () -> {
            try (InputStream dec = this.service.decryptStream(new ByteArrayInputStream(cipher), Integer.valueOf(EncryptionService.ENCRYPTION_FORMAT_V2))) {
                dec.readAllBytes();
            }
        });
    }
}
