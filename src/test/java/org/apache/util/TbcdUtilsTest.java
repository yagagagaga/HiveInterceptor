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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TbcdUtilsTest {

	@Test
	public void testEncode() {
		final byte[] expect = ByteUtils.loadBytes("0x688135532173f2ffffffffffffffffff");

		String a = "8618533512372";
		final byte[] actual = TbcdUtils.encode(a, 16);
		assertArrayEquals(expect, actual);
	}

	@Test
	public void testDecode() {
		final String actual = TbcdUtils.decode(ByteUtils.loadBytes("0x688135532173f2ffffffffffffffffff"));
		final String expect = "8618533512372";
		assertEquals(expect, actual);

		assertEquals("311", TbcdUtils.decode(ByteUtils.loadBytes("0x13f1")));
		assertEquals("0314", TbcdUtils.decode(ByteUtils.loadBytes("0x3041")));

	}
}
