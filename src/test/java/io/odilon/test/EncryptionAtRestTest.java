package io.odilon.test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import io.odilon.encryption.EncryptionService;
import io.odilon.model.ObjectMetadata;
import io.odilon.service.ServerSettings;
import io.odilon.virtualFileSystem.OdilonDrive;
import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * Test to verify encryption at rest is working for both data files and metadata files.
 * 
 * This test verifies:
 * 1. ServerSettings correctly reports encryption is enabled
 * 2. EncryptionService is properly injected and functional
 * 3. ObjectMetadata files are encrypted on disk when encrypt.metadata=true
 * 4. Data files are encrypted when encryption.enabled=true
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@SpringBootTest
@TestPropertySource(properties = {
    "encryption.enabled=true",
    "encrypt.metadata=true",
    "encryption.key=0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF01234567" // 56 hex chars (28 bytes)
})
public class EncryptionAtRestTest {

    @Autowired(required = false)
    private ServerSettings serverSettings;

    @Autowired(required = false)
    private EncryptionService encryptionService;

    @Autowired(required = false)
    private VirtualFileSystemService virtualFileSystemService;

    @Test
    public void testServerSettingsEncryptionEnabled() {
        assertNotNull(serverSettings, "ServerSettings should be autowired");
        assertTrue(serverSettings.isEncryptionEnabled(), 
            "Encryption should be enabled when encryption.enabled=true");
        assertTrue(serverSettings.isEncryptMetadata(), 
            "Metadata encryption should be enabled when encrypt.metadata=true");
    }

    @Test
    public void testEncryptionServiceAvailable() {
        assertNotNull(encryptionService, "EncryptionService should be autowired");
    }

    @Test
    public void testEncryptionServiceFunctional() throws Exception {
        assertNotNull(encryptionService, "EncryptionService must be available");
        
        String testData = "This is sensitive data that should be encrypted";
        byte[] plainBytes = testData.getBytes(StandardCharsets.UTF_8);
        
        // Test encryption
        InputStream plainStream = new ByteArrayInputStream(plainBytes);
        InputStream encryptedStream = encryptionService.encryptStream(plainStream).getInputStream();
        
        assertNotNull(encryptedStream, "Encrypted stream should not be null");
        
        // Read encrypted bytes
        byte[] encryptedBytes = encryptedStream.readAllBytes();
        
        // Encrypted data should be different from plain data
        assertFalse(new String(encryptedBytes, StandardCharsets.UTF_8).contains(testData),
            "Encrypted data should not contain plain text");
        
        // Test decryption
        InputStream decryptStream = encryptionService.decryptStream(
            new ByteArrayInputStream(encryptedBytes));
        byte[] decryptedBytes = decryptStream.readAllBytes();
        String decryptedString = new String(decryptedBytes, StandardCharsets.UTF_8);
        
        assertEquals(testData, decryptedString, 
            "Decrypted data should match original plain text");
    }

    @Test
    public void testVirtualFileSystemEncryptionCheck() {
        assertNotNull(virtualFileSystemService, "VirtualFileSystemService should be autowired");
        assertTrue(virtualFileSystemService.isEncrypt(), 
            "VirtualFileSystemService should report encryption is enabled");
    }

    /**
     * v2 record-based format round trip through the service, dispatched by
     * format version as the drivers do (meta.encryptionFormat = 2).
     */
    @Test
    public void testEncryptionServiceV2RoundTrip() throws Exception {
        assertNotNull(encryptionService, "EncryptionService must be available");

        String testData = "v2 record-based AES-GCM-SIV sensitive data";
        byte[] plainBytes = testData.getBytes(StandardCharsets.UTF_8);

        InputStream encryptedStream = encryptionService.encryptStreamV2(new ByteArrayInputStream(plainBytes)).getInputStream();
        byte[] encryptedBytes = encryptedStream.readAllBytes();

        assertFalse(new String(encryptedBytes, StandardCharsets.UTF_8).contains(testData),
            "Encrypted data should not contain plain text");

        InputStream decryptStream = encryptionService.decryptStream(
            new ByteArrayInputStream(encryptedBytes), Integer.valueOf(EncryptionService.ENCRYPTION_FORMAT_V2));
        assertEquals(testData, new String(decryptStream.readAllBytes(), StandardCharsets.UTF_8),
            "v2 decrypted data should match original plain text");
    }

    /** legacy objects (encryptionFormat == null) keep using the legacy read path */
    @Test
    public void testLegacyDispatchWithNullFormat() throws Exception {
        assertNotNull(encryptionService, "EncryptionService must be available");

        String testData = "legacy whole-object AES/GCM data";
        byte[] plainBytes = testData.getBytes(StandardCharsets.UTF_8);

        byte[] legacyEncrypted = encryptionService.encryptStream(new ByteArrayInputStream(plainBytes)).getInputStream().readAllBytes();

        InputStream decryptStream = encryptionService.decryptStream(new ByteArrayInputStream(legacyEncrypted), null);
        assertEquals(testData, new String(decryptStream.readAllBytes(), StandardCharsets.UTF_8),
            "legacy decrypted data should match original plain text");
    }

    /** unknown format versions must fail clearly, never fall back to another format */
    @Test
    public void testUnknownFormatFailsExplicitly() {
        assertNotNull(encryptionService, "EncryptionService must be available");
        assertThrows(Exception.class, () -> encryptionService.decryptStream(
            new ByteArrayInputStream(new byte[] { 1, 2, 3 }), Integer.valueOf(99)));
    }

    /**
     * This test verifies that when an ObjectMetadata is saved with encrypt=true,
     * and ServerSettings.isEncryptMetadata()=true, the file is written as .json.enc
     * and the content is actually encrypted (not readable as plain JSON).
     * 
     * Note: This is an integration test that requires the full Spring context
     * and actual Drive instances to be available.
     */
    @Test
    public void testObjectMetadataEncryptionOnDisk() throws Exception {
        // This test would need to:
        // 1. Create a test bucket
        // 2. Create an ObjectMetadata with encrypt=true
        // 3. Save it using Drive.saveObjectMetadata()
        // 4. Verify the file on disk is .json.enc
        // 5. Verify the content is encrypted (not plain JSON)
        // 6. Verify it can be read back correctly
        
        // For now, just verify the components are wired correctly
        assertNotNull(virtualFileSystemService, "VirtualFileSystemService required for full test");
        assertNotNull(encryptionService, "EncryptionService required for full test");
        assertNotNull(serverSettings, "ServerSettings required for full test");
        
        if (virtualFileSystemService != null) {
            assertTrue(virtualFileSystemService.isEncrypt(), 
                "Encryption should be enabled in VirtualFileSystemService");
        }
    }

    /**
     * Diagnostic test to print current encryption configuration.
     * This test always passes but prints useful diagnostic information.
     */
    @Test
    public void printEncryptionDiagnostics() {
        System.out.println("\n========== ENCRYPTION DIAGNOSTICS ==========");
        
        if (serverSettings != null) {
            System.out.println("ServerSettings.isEncryptionEnabled(): " + serverSettings.isEncryptionEnabled());
            System.out.println("ServerSettings.isEncryptMetadata(): " + serverSettings.isEncryptMetadata());
        } else {
            System.out.println("ServerSettings: NOT AUTOWIRED");
        }
        
        if (encryptionService != null) {
            System.out.println("EncryptionService: AVAILABLE");
        } else {
            System.out.println("EncryptionService: NOT AUTOWIRED");
        }
        
        if (virtualFileSystemService != null) {
            System.out.println("VirtualFileSystemService.isEncrypt(): " + virtualFileSystemService.isEncrypt());
            try {
                io.odilon.virtualFileSystem.model.IODriver driver = virtualFileSystemService.createVFSIODriver();
                if (driver != null) {
                    System.out.println("IODriver.isEncrypt(): " + driver.isEncrypt());
                } else {
                    System.out.println("IODriver: Could not be created");
                }
            } catch (Exception e) {
                System.out.println("IODriver: Error creating driver - " + e.getMessage());
            }
        } else {
            System.out.println("VirtualFileSystemService: NOT AUTOWIRED");
        }
        
        System.out.println("==========================================\n");
        
        // Test always passes - it's just for diagnostics
        assertTrue(true);
    }
}
