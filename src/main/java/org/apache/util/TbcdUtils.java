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
import java.util.HashMap;
import java.util.Map;

public final class TbcdUtils {
	private static final Map<Character, Byte> map = new HashMap<Character, Byte>() {
		{
			put('0', (byte) 0);
			put('1', (byte) 1);
			put('2', (byte) 2);
			put('3', (byte) 3);
			put('4', (byte) 4);
			put('5', (byte) 5);
			put('6', (byte) 6);
			put('7', (byte) 7);
			put('8', (byte) 8);
			put('9', (byte) 9);
		}
	};

	private TbcdUtils() {
		throw new IllegalStateException("工具类不允许实例化");
	}

	public static byte[] encode(String data, int size) {
		byte[] res = new byte[size];
		Arrays.fill(res, (byte) 0xFF);
		int i = 0;
		boolean low = true;
		final char[] chars = String.valueOf(data).toCharArray();
		for (char c : chars) {
			final Byte b = map.get(c);
			if (low) {
				// 先设置低位
				res[i] = b;
				low = false;
			} else {
				// 再设置高位
				res[i] = (byte) (res[i] | (b << 4));
				low = true;
				i++;
			}
		}

		if (!low) {
			res[i] = (byte) (res[i] | 0xF0);
		}

		return res;
	}

	public static String decode(byte[] data, int offset, int length) {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++) {
			byte b = data[offset + i];
			int lower = b & 0x0f;

			if (lower < 10) {
				sb.append(lower);
			} else {
				break;
			}

			int high = (b >>> 4) & 0x0f;

			if (high < 10) {
				sb.append(high);
			} else {
				break;
			}
		}
		return sb.toString();
	}

	public static String decode(byte[] data, int offset) {
		return decode(data, offset, data.length);
	}

	public static String decode(byte[] data) {
		return decode(data, 0);
	}
}
