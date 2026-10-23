package io.odilon.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.odilon.json.OdilonObjectMapper;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.virtualFileSystem.ObjectMetadataChecksum;
import tools.jackson.databind.ObjectMapper;

/**
 * Plain (no Spring context) round-trip test that reproduces exactly what
 * happens in production:
 *
 * <pre>
 *   RAIDZeroObjectHandler.saveMetadata()   -> builds ObjectMetadata, all fields populated
 *   OdilonDrive.saveObjectMetadata()       -> stamps metaChecksum, serializes to disk
 *   OdilonDrive.getObjectMetadataById()    -> deserializes from disk, verifies metaChecksum
 * </pre>
 *
 * If {@link ObjectMetadataChecksum#verify} returns {@code false} here, the bug
 * is in the checksum/serialization layer itself (a field that does not survive
 * a JSON round trip byte-for-byte). If it returns {@code true} here, the
 * production symptom must come from something outside this pure round trip
 * (encryption, caching, a stale in-memory reference, a second drive, etc.).
 */
public class ObjectMetadataChecksumRoundTripTest {

    private final ObjectMapper mapper = new OdilonObjectMapper();

    /** Builds an ObjectMetadata the same way RAIDZeroObjectHandler.saveMetadata() does. */
    private ObjectMetadata buildSampleMetadata() {
        OffsetDateTime now = OffsetDateTime.now();
        ObjectMetadata meta = new ObjectMetadata(1L, "myObject.txt");
        meta.setFileName("myObject.txt");
        meta.setAppVersion("2.1");
        meta.setContentType("text/plain");
        meta.setEncrypt(true);
        meta.setVault(true);
        meta.setCreationDate(now);
        meta.setLastModified(now);
        meta.setVersioncreationDate(now);
        meta.setVersion(0);
        meta.setSourceLength(1234L);
        meta.setLength(1240L);
        meta.setEtag("abc123");
        meta.setIntegrityCheck(now);
        meta.setSha256("abc123");
        meta.setPublicAccess(false);
        meta.setStatus(ObjectStatus.ENABLED);
        meta.setDrive("drive0");
        meta.setRaid("0");
        meta.setRaidDrives(1);
        List<String> tags = Arrays.asList("tag1", "tag2");
        meta.setCustomTags(tags);
        return meta;
    }

    @Test
    public void testMetaChecksumSurvivesDiskRoundTrip() throws Exception {

        // ---- 1) mimic RAIDZeroObjectHandler.saveMetadata() ----
        ObjectMetadata original = buildSampleMetadata();

        // ---- 2) mimic OdilonDrive.saveObjectMetadata(): stamp checksum, then write ----
        String checksum = ObjectMetadataChecksum.compute(original, mapper);
        original.setMetaChecksum(checksum);

        String jsonWritten = mapper.writeValueAsString(original);
        byte[] bytesWritten = jsonWritten.getBytes("UTF-8");

        System.out.println("---- JSON WRITTEN TO DISK ----");
        System.out.println(jsonWritten);

        // ---- 3) mimic OdilonDrive.getObjectMetadataById(): read back from "disk" ----
        ObjectMetadata reloaded = mapper.readValue(bytesWritten, ObjectMetadata.class);

        String jsonReloadedRaw = mapper.writeValueAsString(reloaded);
        System.out.println("---- JSON IMMEDIATELY RE-SERIALIZED AFTER RELOAD (checksum still set) ----");
        System.out.println(jsonReloadedRaw);

        // ---- 4) verify() recomputes with metaChecksum nulled and compares ----
        boolean ok = ObjectMetadataChecksum.verify(reloaded, mapper);

        if (!ok) {
            // Diagnostic: reproduce exactly what compute() does on both sides and diff the bytes.
            String savedOriginal = original.getMetaChecksum();
            original.setMetaChecksum(null);
            byte[] bytesAtWriteTime = mapper.writeValueAsBytes(original);
            original.setMetaChecksum(savedOriginal);

            String savedReloaded = reloaded.getMetaChecksum();
            reloaded.setMetaChecksum(null);
            byte[] bytesAtReadTime = mapper.writeValueAsBytes(reloaded);
            reloaded.setMetaChecksum(savedReloaded);

            String s1 = new String(bytesAtWriteTime, "UTF-8");
            String s2 = new String(bytesAtReadTime, "UTF-8");

            System.out.println("---- BYTES HASHED AT WRITE TIME (metaChecksum nulled) ----");
            System.out.println(s1);
            System.out.println("---- BYTES HASHED AT READ TIME (metaChecksum nulled) ----");
            System.out.println(s2);

            System.out.println("---- FIRST DIVERGING CHARACTER ----");
            int min = Math.min(s1.length(), s2.length());
            int diffAt = -1;
            for (int i = 0; i < min; i++) {
                if (s1.charAt(i) != s2.charAt(i)) {
                    diffAt = i;
                    break;
                }
            }
            if (diffAt == -1 && s1.length() != s2.length()) {
                diffAt = min;
            }
            if (diffAt >= 0) {
                int from = Math.max(0, diffAt - 40);
                int toW = Math.min(s1.length(), diffAt + 40);
                int toR = Math.min(s2.length(), diffAt + 40);
                System.out.println("index=" + diffAt);
                System.out.println("write..: ..." + s1.substring(from, toW) + "...");
                System.out.println("read...: ..." + s2.substring(from, toR) + "...");
            } else {
                System.out.println("Byte arrays are identical in overlapping range; lengths: "
                        + s1.length() + " vs " + s2.length());
            }
        }

        assertTrue(ok, "metaChecksum must verify true after a plain in-memory JSON round trip");
        assertEquals(original.getMetaChecksum(), reloaded.getMetaChecksum(), "checksum field itself must round-trip unchanged");
    }
}
