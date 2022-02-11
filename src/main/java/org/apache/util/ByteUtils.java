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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

import org.apache.io.Output;

public final class ByteUtils {

	public static char[] hexBytes = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
	public static byte[][][] decimalBytes = new byte[256][256][];
	public static byte[] DigitTens = new byte[1000];
	public static byte[] DigitOnes = new byte[1000];
	public static byte[] DigitHundreds = new byte[1000];

	public static byte[][] decimalChars = new byte[10000][];
	public static byte[][] paddingChars = new byte[10000][];

	static {
		for (int i = 0; i < 256; ++i) {
			for (int j = 0; j < 256; ++j) {
				int value = i << 8 | j;
				decimalBytes[i][j] = Integer.toString(value).getBytes(UTF_8);
			}
		}

		for (int i = 0; i < 10000; ++i) {
			decimalChars[i] = Integer.toString(i).getBytes(UTF_8);
			paddingChars[i] = new byte[4];
			int padding = 4 - decimalChars[i].length;

			int j = 0;
			for (; j < padding; ++j) {
				paddingChars[i][j] = '0';
			}

			for (; j < 4; ++j) {
				paddingChars[i][j] = decimalChars[i][j - padding];
			}
		}

		for (int i = 0; i < 1000; ++i) {
			byte[] buffer = Integer.toString(i).getBytes(UTF_8);

			if (buffer.length == 3) {
				DigitHundreds[i] = buffer[0];
				DigitTens[i] = buffer[1];
				DigitOnes[i] = buffer[2];
			} else if (buffer.length == 2) {
				DigitHundreds[i] = '0';
				DigitTens[i] = buffer[0];
				DigitOnes[i] = buffer[1];
			} else {
				DigitHundreds[i] = '0';
				DigitTens[i] = '0';
				DigitOnes[i] = buffer[0];
			}
		}
	}

	private ByteUtils() {
		throw new IllegalStateException("Please do not instantiate the tool class!");
	}

	/**
	 * @see #dumpBytes(byte[], int, int)
	 */
	public static String dumpBytes(byte[] buffer) {
		if (buffer == null) {
			return null;
		}
		return dumpBytes(buffer, 0, buffer.length);
	}

	/**
	 * 将字节数组转成十六进制的字符串
	 */
	public static String dumpBytes(byte[] buffer, int offset, int length) {
		final StringBuilder sb = new StringBuilder("0x");

		for (int i = offset; i < offset + length; i++) {
			byte b = buffer[i];

			sb.append(hexBytes[(b >>> 4) & 0x0f]);
			sb.append(hexBytes[b & 0x0f]);
		}

		return sb.toString();
	}

	/**
	 * 将十六进制的字符串转成字节数组
	 */
	public static byte[] loadBytes(String dumpStr) {
		final char[] chars = dumpStr.toCharArray();
		byte[] res = new byte[(chars.length - 2) / 2];
		for (int i = 2; i < chars.length; i += 2) {
			final int a = Integer.parseInt(chars[i] + "", 16);
			final int b = Integer.parseInt(chars[i + 1] + "", 16);
			res[i / 2 - 1] = (byte) ((a << 4) + b);
		}
		return res;
	}

// ------------------ bytes --> number ------------------

	/**
	 * @return 无符号长整型
	 * @see #toLong(byte[], int, int)
	 * @deprecated 这个方法算出来的值不对，别用
	 */
	@Deprecated
	public static long toUnsignLong(byte[] buffer, int offset, int length) {

		return toLong(buffer, offset, length) & 0xFFFFFFFFL;
	}

	public static long toLong(byte[] buffer) {
		return toLong(buffer, 0);
	}

	public static long toLong(byte[] buffer, int offset) {
		return toLong(buffer, offset, buffer.length);
	}

	/**
	 * 将字节数组转成数字
	 *
	 * @param buffer 字节数组
	 * @param offset 从哪个偏移量开始转换
	 * @param length 转换多少个字节
	 * @return 有符号长整型
	 */
	public static long toLong(byte[] buffer, int offset, int length) {
		long result = 0;

		for (int i = 0; i < length; ++i) {
			result |= (buffer[offset + i] & 0xffL) << ((length - i - 1) << 3);
		}

		return result;
	}

	public static int toInt(byte[] buffer) {
		return toInt(buffer, 0);
	}

	public static int toInt(byte[] buffer, int offset) {
		return (((buffer[offset]) << 24) |
				((buffer[offset + 1] & 0xff) << 16) |
				((buffer[offset + 2] & 0xff) << 8) |
				((buffer[offset + 3] & 0xff)));
	}

	public static long toUnsignedInt(byte[] buffer) {
		return toUnsignedInt(buffer, 0);
	}

	public static long toUnsignedInt(byte[] buffer, int offset) {
		return toInt(buffer, offset) & 0xFFFFFFFFL;
	}

	public static short toShort(byte[] buffer) {
		return toShort(buffer, 0);
	}

	public static short toShort(byte[] buffer, int offset) {
		return (short) (buffer[offset] << 8 | (buffer[offset + 1] & 0xFF));
	}

	public static int toUnsignShort(byte[] buffer) {
		return toUnsignShort(buffer, 0);
	}

	public static int toUnsignShort(byte[] buffer, int offset) {
		return toShort(buffer, offset) & 0xFFFF;
	}

// ------------------ number --> bytes ------------------

	public static byte[] toBytes(short val) {
		byte[] b = new byte[2];
		b[1] = (byte) val;
		val >>= 8;
		b[0] = (byte) val;
		return b;
	}

	public static byte[] toBytes(int val) {
		byte[] b = new byte[4];
		for (int i = 3; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public static byte[] toBytes(long val) {
		byte[] b = new byte[8];
		for (int i = 7; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	public static byte[] toBytes(long val, int length) {
		byte[] b = new byte[length];
		for (int i = b.length - 1; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

// ------------------ write number as char ------------------

	public static void writeByte(Output output, byte b) {
		for (byte a : decimalChars[b & 0xff]) {
			output.write(a);
		}
	}

	public static void writeInt(Output output, int value) {
		int[] buffer = new int[10];
		if (value < 0) {
			output.write('-');
			value = -value;
		}

		int pos = 0;

		while (value >= 10000) {
			int temp = value / 10000;
			buffer[pos++] = value - temp * 10000;
			value = temp;
		}

		for (byte b : decimalChars[value]) {
			output.write(b);
		}

		for (int i = pos - 1; i >= 0; --i) {
			byte[] chars = paddingChars[buffer[i]];
			output.write(chars[0]);
			output.write(chars[1]);
			output.write(chars[2]);
			output.write(chars[3]);
		}
	}

	public static void writeLong(Output output, long value) {
		int[] buffer = new int[12];
		if (value < 0) {
			output.write('-');
			value = -value;
		}

		int pos = 0;

		while (value >= 10000) {
			long temp = value / 10000;
			buffer[pos++] = (int) (value - temp * 10000);
			value = temp;
		}

		for (byte b : decimalChars[(int) value]) {
			output.write(b);
		}

		for (int i = pos - 1; i >= 0; --i) {
			byte[] chars = paddingChars[buffer[i]];
			output.write(chars[0]);
			output.write(chars[1]);
			output.write(chars[2]);
			output.write(chars[3]);
		}
	}

	public static void writeLong(long value, Output output) {
		if (value < 0) {
			output.write('-');
			value = -value;
		}

		if (value >= 1000) {
			writeLongInternal(value, output);
		} else {
			int v = (int) value;

			if (value >= 100) {
				output.write(DigitHundreds[v]);
			}

			if (value >= 10) {
				output.write(DigitTens[v]);
			}

			output.write(DigitOnes[v]);
		}
	}

	private static void writeIntInternal(int value, Output output) {
		if (value >= 1000000000) {
			int q = value / 1000;
			writeIntInternal(q, output);
			int mod = value - q * 1000;
			output.write(DigitHundreds[mod]);
			output.write(DigitTens[mod]);
			output.write(DigitOnes[mod]);
		} else if (value >= 1000) {
			int q = (int) (value * 42949672L >> 32);
			writeIntInternal(q, output);
			int mod = value - ((q << 6) + (q << 5) + (q << 2));
			output.write(DigitTens[mod]);
			output.write(DigitOnes[mod]);
		} else {
			if (value >= 100) {
				output.write(DigitHundreds[value]);
			}

			if (value >= 10) {
				output.write(DigitTens[value]);
			}

			output.write(DigitOnes[value]);
		}
	}

	private static void writeLongInternal(long value, Output output) {
		if (value >= Integer.MAX_VALUE) {
			long q = value / 100;
			writeLongInternal(q, output);

			int mod = (int) (value - ((q << 6) + (q << 5) + (q << 2)));
			output.write(DigitTens[mod]);
			output.write(DigitOnes[mod]);
		} else {
			writeIntInternal((int) value, output);
		}
	}

	public static void writeDouble(long value, Output output) {
		long digits = value & 0xfffffffffffffL | 0x10000000000000L;
		int e = (int) (((value >> 52) & 0x7ff) - 1023);

		if (e > 20 || e < -10) {
			byte[] bytes = Double.toString(Double.longBitsToDouble(value)).getBytes();
			output.write(bytes, 0, bytes.length);
			return;
		}

		if (value < 0) {
			output.write('-');
		}

		if (e < 0) {
			output.write('0');
		} else {
			writeLong(digits >> (52 - e), output);
		}

		output.write('.');

		long decimal = 0;
		long mask = 0xfffffffffffffL;

		if (e < 0) {
			decimal = (((digits >> 20) * 100000000) >> (32 - e));
		} else {
			decimal = (((digits & (mask >> e)) >> 20) * 100000000) >> (32 - e);
		}

		if (decimal > 0) {
			long temp = decimal;

			while (temp < 10000000) {
				output.write('0');
				temp *= 10;
			}
		}

		writeLong(output, decimal);
	}

// ------------------ bytes --> ip address ------------------

	public static String toIPv4(byte[] data, int offset) {
		Output stream = new Output(32);
		int i = 0;
		while (i < 4) {
			if (i != 0) {
				stream.write('.');
			}
			ByteUtils.writeByte(stream, data[i + offset]);
			i += 1;
		}
		return new String(stream.copyData());
	}

	public static String toIPv6(byte[] buffer, int offset) {
		Output stream = new Output(128);
		int i = 0;
		while (i < 16) {
			if (i > 0) {
				stream.write(':');
			}

			for (int j = 0; j < 2; j++) {
				byte b = buffer[i + offset];

				stream.write(ByteUtils.hexBytes[(b >>> 4) & 0x0f]);
				stream.write(ByteUtils.hexBytes[b & 0x0f]);

				i += 1;
			}
		}
		return new String(stream.copyData());
	}

// ------------------ utils function ------------------

	public static boolean isNotNull(byte[] buffer, int offset, int length) {
		return !isNull(buffer, offset, length);
	}

	public static boolean isNull(byte[] buffer, int offset, int length) {
		int index = 0;

		while (index < length && buffer[index + offset] == -1) {
			index += 1;
		}

		return index == length;
	}

	public static boolean isAllFF(byte[] data, int offset, int length) {
		boolean allFF = true;
		for (int i = 0; i < length; i++) {
			allFF &= data[i + offset] == -1;
		}
		return allFF;
	}

	public static byte[] fillFF(int length) {
		if (length < 0) {
			throw new IllegalArgumentException("length could not less than 0");
		}

		byte[] res = new byte[length];
		Arrays.fill(res, (byte) -1);
		return res;
	}
}
