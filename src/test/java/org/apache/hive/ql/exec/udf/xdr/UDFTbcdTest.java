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
package org.apache.hive.ql.exec.udf.xdr;

import static org.apache.hive.ql.exec.udf.xdr.UDFTbcd.tbcdDes;
import static org.junit.Assert.assertEquals;

import org.apache.io.Output;
import org.junit.Test;

public class UDFTbcdTest {

	@Test
	public void test() {
		byte[] buffer = { 0x10, 0x23, (byte) 0xa4 };
		Output output = new Output(32);
		tbcdDes(output, buffer, 0, 3);

		assertEquals("01324", new String(output.copyData()));
	}
}
