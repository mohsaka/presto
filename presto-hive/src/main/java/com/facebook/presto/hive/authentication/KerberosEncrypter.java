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

import java.util.Arrays;

public class KerberosEncrypter
        implements EncryptionProvider
{
    private static final String ERR_ENCRYPT1 = "Encrypt: Empty message";

    private static final int NO_ERR = 0;

    private static String khex(int i)
    {
        String s = Integer.toHexString(i);
        if (s.length() == 1) {
            s = "0" + s;
        }
        return s.toUpperCase();
    }

    private static byte[] computeK()
    {
        int[] x = {188, 192, 151, 212, 132, 85, 47, 156, 89, 159, 213, 56, 245, 197, 174, 13, 228, 100, 59, 59,
                75, 188, 198, 89, 213, 71, 161, 249, 47, 229, 202, 58};
        byte[] b = new byte[32];
        for (int i = 0; i < 32; i++) {
            b[i] = (byte) x[i];
        }
        return b;
    }

    private static byte[] computeK1(int m)
    {
        byte[] b = new byte[32];
        byte t = (byte) (m % 256);
        Arrays.fill(b, t);
        return b;
    }

    private static byte[] computeK2(byte[] k, byte[] k1)
    {
        byte[] b = new byte[32];
        for (int i = 0; i < 32; i++) {
            b[i] = (byte) (k[i] ^ k1[i]);
        }
        return b;
    }

    private static byte[] align32(byte[] msg)
    {
        int m = msg.length;
        int n = m % 32;
        if (n == 0) {
            return msg;
        }
        else {
            byte[] s = new byte[32 - n];
            return Arrays.copyOf(msg, m + s.length);
        }
    }

    private static byte[] shiftLeft(byte[] blk, int n)
    {
        byte[] a = Arrays.copyOfRange(blk, 0, n);
        byte[] b = Arrays.copyOfRange(blk, n, 32);
        byte[] ret = new byte[32];
        System.arraycopy(b, 0, ret, 0, b.length);
        System.arraycopy(a, 0, ret, b.length, a.length);
        return ret;
    }

    private static byte[] shiftRight(byte[] blk, int n)
    {
        byte[] a = Arrays.copyOfRange(blk, 0, 32 - n);
        byte[] b = Arrays.copyOfRange(blk, 32 - n, 32);
        byte[] ret = new byte[32];
        System.arraycopy(b, 0, ret, 0, b.length);
        System.arraycopy(a, 0, ret, b.length, a.length);
        return ret;
    }

    private static byte[] encryptBlock(byte[] blk, byte[] k2, int i)
    {
        byte[] ret = new byte[32];
        byte[] sblk = shiftLeft(blk, 16 / ((i % 4) + 1));
        for (int j = 0; j < 32; j++) {
            ret[j] = (byte) (sblk[j] ^ k2[i % 32]);
        }
        return ret;
    }

    private static byte[] decryptBlock(byte[] blk, byte[] k2, int i)
    {
        byte[] ret = new byte[32];
        for (int j = 0; j < 32; j++) {
            ret[j] = (byte) (blk[j] ^ k2[i % 32]);
        }
        byte[] sblk = shiftRight(ret, 16 / ((i % 4) + 1));
        return sblk;
    }

    public static String k256Encrypt(String msgBuff)
    {
        if (msgBuff.isEmpty()) {
            return ERR_ENCRYPT1;
        }
        byte[] msg2 = align32(msgBuff.getBytes());
        int m32 = msg2.length;
        byte[] h = new byte[2];
        h[0] = (byte) ((m32 & 0xFF00) >> 8);
        h[1] = (byte) (m32 & 0xFF);
        byte[] k = computeK();
        byte[] k1 = computeK1(m32);
        byte[] k2 = computeK2(k, k1);
        byte[] rblk = Arrays.copyOf(h, h.length);
        for (int i = 0; i < m32 / 32; i++) {
            byte[] blk = Arrays.copyOfRange(msg2, 32 * i, (32 * i) + 32);
            byte[] eblk = encryptBlock(blk, k2, i);
            rblk = Arrays.copyOf(rblk, rblk.length + eblk.length);
            System.arraycopy(eblk, 0, rblk, rblk.length - eblk.length, eblk.length);
        }
        StringBuilder ret = new StringBuilder();
        for (byte b : rblk) {
            ret.append(khex(b & 0xFF));
        }
        return ret.toString();
    }

    @Override
    public byte[] k256Encrypt(byte[] clearBytes)
    {
        return new byte[0];
    }

    public byte[] k256Decrypt(String encMsgStr)
    {
        int ub = Integer.parseInt(encMsgStr.substring(0, 2), 16);
        int lb = Integer.parseInt(encMsgStr.substring(2, 4), 16);
        int m = ub * (0x100) + lb;

        int m32 = (encMsgStr.length() - 4) / 2;
        byte[] msgArray = new byte[m32];
        byte[] k = computeK();
        byte[] k1 = computeK1(m);
        byte[] k2 = computeK2(k, k1);
        String hmsg = encMsgStr.substring(4);

        for (int i = 0; i < hmsg.length() / 2; i++) {
            int v = Integer.parseInt(hmsg.substring(2 * i, 2 * i + 2), 16);
            msgArray[i] = (byte) v;
        }
        byte[] ret = new byte[0];
        for (int i = 0; i < m32 / 32; i++) {
            byte[] blk = Arrays.copyOfRange(msgArray, 32 * i, (32 * i) + 32);
            byte[] dblk = decryptBlock(blk, k2, i);

            ret = Arrays.copyOf(ret, ret.length + dblk.length);

            System.arraycopy(dblk, 0, ret, ret.length - dblk.length, dblk.length);
        }
        byte[] retShort = Arrays.copyOf(ret, m);
        return retShort;
    }
}
