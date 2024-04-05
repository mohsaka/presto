/*
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
package com.facebook.presto.hive.authentication;

/**
 * This is the interface implemented by encryption providers.
 * <p>
 * {@link #k256Encrypt(byte[])} and {@link #k256Decrypt(string)} are the encryption and
 * decryption operations for the provider.
 * </p>
 * <p>
 * {@link #getAlias()} must return a short name (usually an acronym) that will
 * uniquely identify the encryption provider. This alias can be used by callers
 * to mark the encrypted data with a {@code{<alias value>}} prefix so one can
 * figure out which provider was used to encrypt the data.
 * </p>
 * The InformationServer utilize the Java service provider pattern to load the
 * encryption provider from the classpath. So the user should also make sure the
 * configuration file
 * META-INF/services/com.ibm.iis.spi.security.crypto.EncryptionProvider is
 * created and bundled (see
 * http://download.oracle.com/javase/1.4.2/docs/guide/jar/jar.html#Service%20Provider)
 */
public interface EncryptionProvider
{
    /**
     * The encrypt operation takes the byte[] and converts it to the encrypted
     * byte[]
     *
     * @param clearBytes
     *            The byte array to be encrypted
     * @return byte[] The encrypted byte array
     **/
    byte[] k256Encrypt(byte[] clearBytes);

    /**
     * The decrypt operation takes the encrypted byte[] and converts it to the
     * decrypted byte[]
     *
     * @param encMsgStr
     *            The byte array to be decrypted
     * @return byte[] The decrypted byte array
     **/
    //byte[] decrypt(byte[] encryptedBytes) throws DecryptException;

    byte[] k256Decrypt(String encMsgStr);
}
