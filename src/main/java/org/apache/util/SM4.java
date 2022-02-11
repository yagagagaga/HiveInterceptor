/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.util;

import java.util.Arrays;

@Deprecated
public class SM4 {
	private static final int ENCRYPT = 1;
	private static final int DECRYPT = 0;
	private static final int BLOCK = 16;
	private final byte[] lowerHex = {
			0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
			0x61, 0x62, 0x63, 0x64, 0x65, 0x66
	};
	private final byte[] inputBuffer = new byte[16];
	private final byte[] outputBuffer = new byte[16];
	private final int[] round_key = new int[32];
	private final int[] CK = { 0x00070e15, 0x1c232a31, 0x383f464d, 0x545b6269,
			0x70777e85, 0x8c939aa1, 0xa8afb6bd, 0xc4cbd2d9, 0xe0e7eef5,
			0xfc030a11, 0x181f262d, 0x343b4249, 0x50575e65, 0x6c737a81,
			0x888f969d, 0xa4abb2b9, 0xc0c7ced5, 0xdce3eaf1, 0xf8ff060d,
			0x141b2229, 0x30373e45, 0x4c535a61, 0x686f767d, 0x848b9299,
			0xa0a7aeb5, 0xbcc3cad1, 0xd8dfe6ed, 0xf4fb0209, 0x10171e25,
			0x2c333a41, 0x484f565d, 0x646b7279 };
	int[] Sbox = { 0xd6, 0x90, 0xe9, 0xfe,
			0xcc, 0xe1, 0x3d, 0xb7, 0x16, 0xb6,
			0x14, 0xc2, 0x28, 0xfb, 0x2c, 0x05, 0x2b, 0x67,
			0x9a, 0x76, 0x2a, 0xbe, 0x04, 0xc3,
			0xaa, 0x44, 0x13, 0x26, 0x49, 0x86, 0x06,
			0x99, 0x9c, 0x42, 0x50, 0xf4, 0x91,
			0xef, 0x98, 0x7a, 0x33, 0x54, 0x0b, 0x43,
			0xed, 0xcf, 0xac, 0x62, 0xe4,
			0xb3, 0x1c, 0xa9, 0xc9, 0x08, 0xe8,
			0x95, 0x80, 0xdf, 0x94, 0xfa,
			0x75, 0x8f, 0x3f, 0xa6, 0x47, 0x07, 0xa7,
			0xfc, 0xf3, 0x73, 0x17, 0xba, 0x83,
			0x59, 0x3c, 0x19, 0xe6, 0x85, 0x4f, 0xa8,
			0x68, 0x6b, 0x81, 0xb2, 0x71, 0x64, 0xda,
			0x8b, 0xf8, 0xeb, 0x0f, 0x4b, 0x70, 0x56,
			0x9d, 0x35, 0x1e, 0x24, 0x0e, 0x5e, 0x63, 0x58, 0xd1,
			0xa2, 0x25, 0x22, 0x7c, 0x3b, 0x01, 0x21, 0x78, 0x87,
			0xd4, 0x00, 0x46, 0x57, 0x9f, 0xd3, 0x27,
			0x52, 0x4c, 0x36, 0x02, 0xe7, 0xa0, 0xc4,
			0xc8, 0x9e, 0xea, 0xbf, 0x8a,
			0xd2, 0x40, 0xc7, 0x38, 0xb5, 0xa3,
			0xf7, 0xf2, 0xce, 0xf9, 0x61, 0x15,
			0xa1, 0xe0, 0xae, 0x5d, 0xa4,
			0x9b, 0x34, 0x1a, 0x55, 0xad, 0x93, 0x32,
			0x30, 0xf5, 0x8c, 0xb1, 0xe3, 0x1d,
			0xf6, 0xe2, 0x2e, 0x82, 0x66, 0xca,
			0x60, 0xc0, 0x29, 0x23, 0xab, 0x0d, 0x53, 0x4e, 0x6f,
			0xd5, 0xdb, 0x37, 0x45, 0xde, 0xfd,
			0x8e, 0x2f, 0x03, 0xff, 0x6a, 0x72, 0x6d, 0x6c, 0x5b,
			0x51, 0x8d, 0x1b, 0xaf, 0x92, 0xbb,
			0xdd, 0xbc, 0x7f, 0x11, 0xd9, 0x5c, 0x41,
			0x1f, 0x10, 0x5a, 0xd8, 0x0a, 0xc1, 0x31,
			0x88, 0xa5, 0xcd, 0x7b, 0xbd, 0x2d,
			0x74, 0xd0, 0x12, 0xb8, 0xe5, 0xb4,
			0xb0, 0x89, 0x69, 0x97, 0x4a, 0x0c,
			0x96, 0x77, 0x7e, 0x65, 0xb9, 0xf1, 0x09,
			0xc5, 0x6e, 0xc6, 0x84, 0x18, 0xf0,
			0x7d, 0xec, 0x3a, 0xdc, 0x4d, 0x20, 0x79,
			0xee, 0x5f, 0x3e, 0xd7, 0xcb, 0x39, 0x48 };

	protected static int toDigit(byte ch) {
		return Character.digit(ch, 16);
	}

	private int rotl(int x, int y) {
		return x << y | x >>> (32 - y);
	}

	private int byteSub(int A) {
		return (Sbox[A >>> 24]) << 24
				| (Sbox[A >> 16 & 0xFF]) << 16
				| (Sbox[A >> 8 & 0xFF]) << 8 | (Sbox[A & 0xFF]);
	}

	private int l1(int B) {
		return B ^ rotl(B, 2) ^ rotl(B, 10) ^ rotl(B, 18) ^ rotl(B, 24);
	}

	private int l2(int B) {
		return B ^ rotl(B, 13) ^ rotl(B, 23);
	}

	void sms4Crypt(byte[] Input, byte[] Output, int[] rk) {
		int r, mid, x0, x1, x2, x3;

		x0 = (Input[0] << 24) | ((Input[1] & 0xff) << 16) | ((Input[2] & 0xff) << 8) |
				((Input[3] & 0xff));
		x1 = (Input[4] << 24) | ((Input[5] & 0xff) << 16) | ((Input[6] & 0xff) << 8) |
				((Input[7] & 0xff));
		x2 = (Input[8] << 24) | ((Input[9] & 0xff) << 16) | ((Input[10] & 0xff) << 8) |
				((Input[11] & 0xff));
		x3 = (Input[12] << 24) | ((Input[13] & 0xff) << 16) | ((Input[14] & 0xff) << 8) |
				((Input[15] & 0xff));

		for (r = 0; r < 32; r += 4) {
			mid = x1 ^ x2 ^ x3 ^ rk[r];
			mid = byteSub(mid);
			x0 ^= l1(mid); // x4

			mid = x2 ^ x3 ^ x0 ^ rk[r + 1];
			mid = byteSub(mid);
			x1 ^= l1(mid); // x5

			mid = x3 ^ x0 ^ x1 ^ rk[r + 2];
			mid = byteSub(mid);
			x2 ^= l1(mid); // x6

			mid = x0 ^ x1 ^ x2 ^ rk[r + 3];
			mid = byteSub(mid);
			x3 ^= l1(mid); // x7
		}

		Output[0] = (byte) (x3 >> 24);
		Output[1] = (byte) (x3 >> 16);
		Output[2] = (byte) (x3 >> 8);
		Output[3] = (byte) x3;

		Output[4] = (byte) (x2 >> 24);
		Output[5] = (byte) (x2 >> 16);
		Output[6] = (byte) (x2 >> 8);
		Output[7] = (byte) x2;

		Output[8] = (byte) (x1 >> 24);
		Output[9] = (byte) (x1 >> 16);
		Output[10] = (byte) (x1 >> 8);
		Output[11] = (byte) x1;

		Output[12] = (byte) (x0 >> 24);
		Output[13] = (byte) (x0 >> 16);
		Output[14] = (byte) (x0 >> 8);
		Output[15] = (byte) x0;
	}

	private void sms4Keyext(byte[] Key, int[] rk, int CryptFlag) {
		int r, mid;
		int x0, x1, x2, x3;

		x0 = Key[0] << 24;
		x0 |= Key[1] << 16;
		x0 |= Key[2] << 8;
		x0 |= Key[3];

		x1 = Key[4] << 24;
		x1 |= Key[5] << 16;
		x1 |= Key[6] << 8;
		x1 |= Key[7];

		x2 = Key[8] << 24;
		x2 |= Key[9] << 16;
		x2 |= Key[10] << 8;
		x2 |= Key[11];

		x3 = Key[12] << 24;
		x3 |= Key[13] << 16;
		x3 |= Key[14] << 8;
		x3 |= Key[15];

		x0 ^= 0xa3b1bac6;
		x1 ^= 0x56aa3350;
		x2 ^= 0x677d9197;
		x3 ^= 0xb27022dc;

		for (r = 0; r < 32; r += 4) {
			mid = x1 ^ x2 ^ x3 ^ CK[r];
			mid = byteSub(mid);
			rk[r] = x0 ^= l2(mid); // rk0=K4

			mid = x2 ^ x3 ^ x0 ^ CK[r + 1];
			mid = byteSub(mid);
			rk[r + 1] = x1 ^= l2(mid); // rk1=K5

			mid = x3 ^ x0 ^ x1 ^ CK[r + 2];
			mid = byteSub(mid);
			rk[r + 2] = x2 ^= l2(mid); // rk2=K6

			mid = x0 ^ x1 ^ x2 ^ CK[r + 3];
			mid = byteSub(mid);
			rk[r + 3] = x3 ^= l2(mid); // rk3=K7
		}

		// 解密时轮密钥使用顺序：rk31,rk30,...,rk0
		if (CryptFlag == DECRYPT) {
			for (r = 0; r < 16; r++) {
				mid = rk[r];
				rk[r] = rk[31 - r];
				rk[31 - r] = mid;
			}
		}
	}

	public byte[] encrypt(byte[] in, int offset, int inLen, byte[] key) {
		int roundLen = ((inLen & 0x0f) > 0) ? ((inLen & 0xfff0) + 16) : inLen;
		byte[] input = new byte[roundLen];
		byte[] output = new byte[roundLen];
		System.arraycopy(in, offset, input, 0, inLen);
		Arrays.fill(input, inLen, roundLen, (byte) 32);
		sms4(input, roundLen, key, output, ENCRYPT);
		return encodeHex(output);
	}

	private byte[] encodeHex(byte[] in) {
		int len = in.length;
		byte[] hex = new byte[len << 1];

		int i = 0;
		for (int j = 0; i < len; ++i) {
			hex[j++] = lowerHex[(in[i] >> 4) & 0x0f];
			hex[j++] = lowerHex[15 & in[i]];
		}

		return hex;
	}

	private byte[] decodeHex(byte[] in, int length) {

		byte[] out = new byte[length >> 1];
		int i = 0;

		for (int j = 0; j < length; ++i) {
			int f = toDigit(in[j]) << 4;
			++j;
			f |= toDigit(in[j]);
			++j;
			out[i] = (byte) (f & 255);
		}

		return out;
	}

	public byte[] decrypt(byte[] in, int inLen, byte[] key) {
		byte[] input = decodeHex(in, inLen);
		byte[] output = new byte[input.length];
		sms4(input, input.length, key, output, DECRYPT);

		int i = input.length - 1;
		for (; i >= 0; --i) {
			if (output[i] != 32) {
				break;
			}
		}

		return Arrays.copyOf(output, i + 1);
	}

	public int sms4(byte[] in, int inLen, byte[] key, byte[] out, int CryptFlag) {
		int point = 0;
		// int[] round_key={0};
		sms4Keyext(key, round_key, CryptFlag);
		// byte[] output = new byte[16];

		while (inLen >= BLOCK) {
			System.arraycopy(in, 0, inputBuffer, 0, 16);
			// input = Arrays.copyOfRange(in, point, point + 16);
			// output=Arrays.copyOfRange(out, point, point+16);
			sms4Crypt(inputBuffer, outputBuffer, round_key);
			System.arraycopy(outputBuffer, 0, out, point, BLOCK);
			inLen -= BLOCK;
			point += BLOCK;
		}

		return 0;
	}
}