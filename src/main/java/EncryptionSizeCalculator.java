
// EncryptionSizeCalculator.java (for AES-GCM-SIV)
public final class EncryptionSizeCalculator {
	private static final int GCM_SIV_TAG_LENGTH = 16; // bytes

	private EncryptionSizeCalculator() {
	}

	public static long encryptedPayloadSizeGcmSiv(long plainSize) {
		return plainSize + GCM_SIV_TAG_LENGTH;
	}
}
