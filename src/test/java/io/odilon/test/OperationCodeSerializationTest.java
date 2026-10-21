package io.odilon.test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.odilon.json.OdilonObjectMapper;
import io.odilon.virtualFileSystem.OdilonVirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.OperationCode;
import tools.jackson.databind.ObjectMapper;

/**
 * Plain (no Spring context) round-trip test for OperationCode serialization.
 */
public class OperationCodeSerializationTest {

    private final ObjectMapper mapper = new OdilonObjectMapper();

    /** The exact JSON written to disk by the buggy code */
    private static final String LEGACY_JSON =
        "{\"id\":\"26261574557958\",\"bucketId\":1,\"bucketName\":\"dev-test\"," +
        "\"objectName\":\"Tarjeta de Circulacion _ JLH-678-4668\"," +
        "\"raid\":\"RAID_6\",\"UUID\":\"o:1:Tarjeta de Circulacion _ JLH-678-4668\"," +
        "\"operation\":\"OperationCode{\\\"name\\\": \\\"create_object\\\", \\\"code\\\": 10, \\\"description\\\": \\\"create_object\\\"}\"," +
        "\"redundancyLevel\":\"RAID_6\",\"replicates\":true," +
        "\"timestamp\":\"2026-05-31T11:09:43.935104-03:00\",\"version\":0}";

    @Test
    public void testFromJsonOldFormat() {
        String old = "OperationCode{\"name\": \"create_object\", \"code\": 10, \"description\": \"create_object\"}";
        OperationCode code = OperationCode.fromJson(old);
        assertNotNull(code, "fromJson must not return null for old format");
        assertEquals(OperationCode.CREATE_OBJECT, code);
    }

    @Test
    public void testFromJsonNewFormat() {
        OperationCode code = OperationCode.fromJson("create_object");
        assertNotNull(code);
        assertEquals(OperationCode.CREATE_OBJECT, code);
    }

    @Test
    public void testAllOperationCodesRoundTrip() throws Exception {
        for (OperationCode expected : OperationCode.values()) {
            // serialize
            String json = mapper.writeValueAsString(expected);
            // should be a plain quoted string e.g. "create_object"
            assertFalse(json.contains("OperationCode{"),
                "Serialized form should NOT contain toString() format for: " + expected);
            // deserialize
            OperationCode actual = mapper.readValue(json, OperationCode.class);
            assertNotNull(actual, "Deserialized OperationCode must not be null for: " + expected);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testDeserializeLegacyJournalFile() throws Exception {
        OdilonVirtualFileSystemOperation op =
            mapper.readValue(LEGACY_JSON, OdilonVirtualFileSystemOperation.class);

        assertNotNull(op, "Deserialized operation must not be null");
        assertNotNull(op.getOperationCode(),
            "operationCode must not be null — @JsonCreator is not being called");
        assertEquals(OperationCode.CREATE_OBJECT, op.getOperationCode());
        // this was the NPE line
        assertDoesNotThrow(() -> op.getOperationCode().isReplicates(),
            "isReplicates() must not throw NPE");
        assertTrue(op.isReplicates());
    }
}
