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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ByteUtilsTest {
	@Test
	public void testShortToBytes() {
		short val;
		byte[] actual;

		val = Short.parseShort("F", 16);
		actual = ByteUtils.toBytes(val);
		assertEquals((byte) 0x00, actual[0]);
		assertEquals((byte) 0x0F, actual[1]);

		val = Short.parseShort("-0001", 16);
		actual = ByteUtils.toBytes(val);
		assertEquals((byte) 0xFF, actual[0]);
		assertEquals((byte) 0xFF, actual[1]);

		val = 0;
		actual = ByteUtils.toBytes(val);
		assertEquals((byte) 0, actual[0]);
		assertEquals((byte) 0, actual[1]);

		val = Short.MAX_VALUE; // 7FFF
		actual = ByteUtils.toBytes(val);
		assertEquals((byte) 0x7F, actual[0]);
		assertEquals((byte) 0xFF, actual[1]);

		val = Short.MIN_VALUE; // -8000
		actual = ByteUtils.toBytes(val);
		assertEquals((byte) 0x80, actual[0]);
		assertEquals((byte) 0x00, actual[1]);
	}
}
