package io.odilon.encryption;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import jakarta.annotation.PostConstruct;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.service.ServerSettings;
import io.odilon.service.util.ByteToString;

/**
 * <p>The variable <b>encryption.key</b> in <b>odilon.properties</b> contain a AES key used to decrypt the
 * Server Master Key on server's startup. </p>
 * 
 * <p>The Master Key is unique for each server and can not be changed.</p>
 *  
 * <p>It used by the {@link EncryptionService} to encrypt/decrypt files. Strictly speaking the
 * Master Key is used by the EncryptionService to encrypt/decrypt the key used to encrypt/decrypt every 
 * Object. Each Object has its own unique encryption key</p>
 *
 *<p>The procedure is:</p>
 *
 * <b>SERVER STARTUP</b>
 * <ul>
 * <li>Odilon decrypts the MasterKEy using the key provided in variable encryption.key in odilon.properties</li>
 * </ul>
 * 
 * 
 * <br/>
 * <b>PUT OBJECT</b>
 * <ul>
 * <li>Odilon generates a new key for every Object (objKey)</li>
 * <li>The Object is encrypted using AES with key objKey</li>
 * <li>The obkKey is encrypted by {@link OdilonKeyEncryptorService} using AES with the server Master Key or by the KMS if enabled, and saved in disk as a prefix of the Object</li>
 * </ul>
 * 
 * 
 * <br/>
 * 
 * <b>GET OBJECT</b>
 * </ul>
 * <li>Odilon reads the stored Object and extracts the objKey</li>
 * <li>objKey is decrypted using MasterKey or KMS if enabled</li>
 * <li>Object is decrypted using objKey</li>
 * </ul>
 *
 *<br/>
 *<br/>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class MasterKeyService extends BaseService implements KeyEncryptor {

	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(OdilonKeyEncryptorService.class.getName());

	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	static final String keyEncryptionAlgorithm = ServerConstant.DEFAULT_ENCRYPT_ALGORITHM;
	static final String keyAlgorithm = ServerConstant.DEFAULT_KEY_ALGORITHM;
	
	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;
	
   @JsonIgnore
   private byte[] keyToEncryptMasterKey; 
   
   @JsonIgnore
   private Cipher enc;
   
   @JsonIgnore
   private Cipher dec;
   
   public MasterKeyService(ServerSettings serverSettings) {
	this.serverSettings=serverSettings;
   }
    
   
    /**
     * 
     */
    @Override
    public byte[] encryptKey(byte[] keyWithSalt) {
        try {
            return processBytes(keyWithSalt,Cipher.ENCRYPT_MODE, keyToEncryptMasterKey);
        } catch (Exception e){
            throw new InternalCriticalException(e, "encryptKey");
        }
    }

    /**
     * 
     */
    @Override
    public byte[] decryptKey(byte[] keyWithSalt) {
        try {
            return processBytes(keyWithSalt,Cipher.DECRYPT_MODE, keyToEncryptMasterKey);
        } catch (Exception e){
        	throw new InternalCriticalException(e, "decryptKey");
        }
    }
    
    
    /**
     * 
     * @param encKey
     */
    public synchronized void setKeyToEncryptMasterKey( byte [] encKey) {
    	
    	try {
			
			this.keyToEncryptMasterKey = encKey;

			SecretKeySpec secretKeySpec = new SecretKeySpec(keyToEncryptMasterKey, keyAlgorithm);
			
			this.enc = Cipher.getInstance(keyEncryptionAlgorithm);
			this.enc.init(Cipher.ENCRYPT_MODE, secretKeySpec);
		
			this.dec = Cipher.getInstance(keyEncryptionAlgorithm);
			this.dec.init(Cipher.DECRYPT_MODE, secretKeySpec);

		
		} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
			setStatus(ServiceStatus.STOPPED);
			throw new InternalCriticalException(e, "setKeyToEncryptMasterKey");
		}
	}
    
    
    /**
     * 
     */
	@PostConstruct
	protected void onInitialize() {

		synchronized (this) {
				
				setStatus(ServiceStatus.STARTING);

				if (this.serverSettings.getEncryptionKey()!=null) {
					setKeyToEncryptMasterKey(ByteToString.hexStringToByte(serverSettings.getEncryptionKey().trim()) );
				}
				
				startuplogger.debug("Started -> " + this.getClass().getSimpleName());
				setStatus(ServiceStatus.RUNNING);

		}
	}
		   
	/**
	 * @param bytes
	 * @param encryptMode
	 * @param key
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeyException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 */
    private byte[] processBytes(byte[] bytes, int encryptMode, byte[] key) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        if (encryptMode==Cipher.ENCRYPT_MODE)
        	return this.enc.doFinal(bytes);
        else
        	return this.dec.doFinal(bytes);
    }
	


}
