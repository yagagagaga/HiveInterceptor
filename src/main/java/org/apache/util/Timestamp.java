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

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Timestamp {
	private static long[][] timestampPerMonth = new long[50][];

	static {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		int i = 0;
		while (i < 50) {
			timestampPerMonth[i] = new long[12];

			int j = 0;
			while (j < 12) {
				try {
					timestampPerMonth[i][j] = format
							.parse(String.format("%4d%2d01000000", (i + 2000), (j + 1)))
							.getTime();
					j += 1;
				} catch (ParseException e) {
					throw new IllegalStateException(e);
				}
			}

			i += 1;
		}
	}

	public static int toInt(byte[] buffer, int offset, int length) {
		int result = 0;
		int i = offset;
		int end = offset + length;
		while (i < end) {
			int b = buffer[i] - '0';
			assert (b >= 0 && b < 10);
			result = result * 10 + b;
			i += 1;
		}

		return result;
	}

	public static long toTimestamp(byte[] buffer, int offset, int length) {
		int year = toInt(buffer, offset, 4);
		assert (buffer[offset + 4] == '-');
		int month = toInt(buffer, offset + 5, 2);
		assert (buffer[offset + 7] == '-');
		int day = toInt(buffer, offset + 8, 2);
		assert (buffer[offset + 10] == ' ');
		int hour = toInt(buffer, offset + 11, 2);
		assert (buffer[offset + 13] == ':');
		int minute = toInt(buffer, offset + 14, 2);
		assert (buffer[offset + 16] == ':');
		int second = toInt(buffer, offset + 17, 2);
		int yearPos = year - 2000;

		if (yearPos < 0 || yearPos >= 50) {
			return 0;
		} else {
			long ts = timestampPerMonth[yearPos][month - 1];
			return ts + ((day - 1) * 24 + hour) * 3600 * 1000L + minute * 60 * 1000 + second * 1000;
		}
	}
}
