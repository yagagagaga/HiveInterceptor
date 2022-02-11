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

/**
 * TLV 字段的格式如下：
 *
 * <pre>
 * <code>
 *      bit 8  7  6  5  4  3  2  1
 *        +------------------------+
 * Byte 1 |                 tag    |  ==> tag    12bit
 *        +------------+           |
 * Byte 2 |  format    |           |  ==> format 4bit
 *        +------------+-----------+
 * Byte 3 |                        |
 *        +         length         +  ==> length 16bit or 0bit
 * Byte 4 |                        |
 *        +------------------------+
 * Byte 5 |          value         |  ==> value  ?
 *        +------------------------+
 * </code>
 * </pre>
 */
public final class TlvUtils {
	private TlvUtils() {
		throw new IllegalStateException("工具类不允许实例化");
	}

	/**
	 * 获取 TLV 字段的 Tag 值
	 *
	 * @param tlvField 一串二进制数
	 * @param offset   TLV 字段所在的偏移量
	 */
	public static int getTLVTag(byte[] tlvField, int offset) {
		byte[] res = new byte[2];
		res[1] = tlvField[offset];
		res[0] = (byte) (tlvField[offset + 1] & 0x0F);
		return ByteUtils.toUnsignShort(res, 0);
	}

	/**
	 * 获取 TLV 字段 Value 的偏移量
	 *
	 * @param tlvField 一串二进制数
	 * @param offset   TLV 字段所在的偏移量
	 */
	public static int getTLVValueOffset(byte[] tlvField, int offset) {
		final int format = getFormatValue(tlvField, offset);
		if (format == 0) {
			// format 为 0，有 Length 字段，相当于 LV
			return getLVValueOffset(tlvField, offset + 2);
		} else {
			// format 为 0，没有 Length 字段，相当于 TV
			return getTVValueOffset(tlvField, offset);
		}
	}

	/**
	 * 获取 TLV 字段 Value 的长度
	 *
	 * @param tlvField 一串二进制数
	 * @param offset   TLV 字段所在的偏移量
	 */
	public static int getTLVValueLength(byte[] tlvField, int offset) {
		final int format = getFormatValue(tlvField, offset);
		if (format == 0) {
			// format 为 0，有 Length 字段，相当于 LV
			return getLVValueLength(tlvField, offset + 2);
		} else {
			// format 为 0，没有 Length 字段，相当于 TV
			return getTVValueLength(format);
		}
	}

	/**
	 * 获取 TV 字段 Value 的偏移量
	 *
	 * @param tvField 一串二进制数
	 * @param offset  TV 字段所在的偏移量
	 */
	public static int getTVValueOffset(byte[] tvField, int offset) {
		return offset + 2;
	}

	/**
	 * 获取 TV 字段 Value 的长度
	 *
	 * @param tvField 一串二进制数
	 * @param offset  TV 字段所在的偏移量
	 */
	public static int getTVValueLength(byte[] tvField, int offset) {
		final int format = getFormatValue(tvField, offset);
		return getTVValueLength(format);
	}

	private static int getTVValueLength(int format) {
		switch (format) {
		case 1:
			return 1;
		case 2:
			return 2;
		case 3:
			return 3;
		case 4:
			return 4;
		case 5:
			return 5;
		case 6:
			return 6;
		case 7:
			return 8;
		case 8:
			return 16;
		case 9:
			return 32;
		case 10:
			return 64;
		case 11:
			return 128;
		case 12:
			return 256;
		default:
			throw new IllegalStateException("暂不支持 format = " + format + " 的值");
		}
	}

	/**
	 * 获取 LV 字段 Value 的偏移量
	 *
	 * @param lvField 一串二进制数
	 * @param offset  LV 字段所在的偏移量
	 */
	public static int getLVValueOffset(byte[] lvField, int offset) {
		return offset + 2;
	}

	/**
	 * 获取 LV 字段 Value 的长度
	 *
	 * @param lvField 一串二进制数
	 * @param offset  LV 字段所在的偏移量
	 */
	public static int getLVValueLength(byte[] lvField, int offset) {
		return ByteUtils.toUnsignShort(lvField, offset);
	}

	/**
	 * 获取 TLV 字段 Format 的值
	 *
	 * @param tlvField 一串二进制数
	 * @param offset   TLV 字段所在的偏移量
	 */
	public static int getFormatValue(byte[] tlvField, int offset) {
		return (tlvField[offset + 1] & 0xFF) >> 4;
	}

	/**
	 * 将数据封装成 TV 格式的数据
	 *
	 * @param data 输入的数据
	 */
	public static byte[] tv(short tag, byte[] data) {
		final int length = data.length;
		short format = calcTlvFormat(data);
		if (format == 0) {
			throw new IllegalStateException("目前不支持此长度的数据做 TV 编码：length = " + length);
		}
		byte[] res = new byte[2 + length];
		final byte[] tagBytes = ByteUtils.toBytes(tag);
		res[0] = tagBytes[1];
		res[1] = (byte) ((tagBytes[0] & 0x0F) | (format << 4));
		System.arraycopy(data, 0, res, 2, length);
		return res;
	}

	/**
	 * 将数据封装成 LV 格式的数据
	 *
	 * @param data 输入的数据
	 */
	public static byte[] lv(byte[] data) {
		final int length = data.length;
		final byte[] res = new byte[length + 2];
		final byte[] bytes = ByteUtils.toBytes(length);
		res[0] = bytes[2];
		res[1] = bytes[3];

		System.arraycopy(data, 0, res, 2, length);
		return res;
	}

	/**
	 * 将数据封装成 TLV 格式的数据
	 *
	 * @param data 输入的数据
	 */
	public static byte[] tlv(short tag, byte[] data) {

		final short format = calcTlvFormat(data);
		if (format != 0) {
			return tv(tag, data);
		}
		final byte[] tagBytes = ByteUtils.toBytes(tag);

		byte[] res = new byte[4 + data.length];
		res[0] = tagBytes[1];
		res[1] = (byte) (tagBytes[0] & 0x0F);

		final int length = data.length;
		final byte[] bytes = ByteUtils.toBytes(length);
		res[2] = bytes[2];
		res[3] = bytes[3];

		System.arraycopy(data, 0, res, 4, length);
		return res;
	}

	private static short calcTlvFormat(byte[] data) {
		short format;
		switch (data.length) {
		case 1:
			format = 1;
			break;
		case 2:
			format = 2;
			break;
		case 3:
			format = 3;
			break;
		case 4:
			format = 4;
			break;
		case 5:
			format = 5;
			break;
		case 6:
			format = 6;
			break;
		case 8:
			format = 7;
			break;
		case 16:
			format = 8;
			break;
		case 32:
			format = 9;
			break;
		case 64:
			format = 10;
			break;
		case 128:
			format = 11;
			break;
		case 256:
			format = 12;
			break;
		default:
			format = 0;
		}
		return format;
	}
}
