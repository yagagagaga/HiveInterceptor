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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.util.ByteUtils;
import org.junit.Test;

public class UDFBytesToIPTest extends UDFBaseTest {

	@Override
	public GenericUDF createUDF() {
		return new UDFBytesToIP();
	}

	@Override
	public ObjectInspector[] argumentsOIs() {
		// 从javaStringObjectInspector改为了这个具体的传参
		return new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.writableByteObjectInspector };
	}

	@Test
	public void testUdf_bytesToIPV4_ok() throws HiveException {
		// 检查ipv4
		final ImmutableBytesWritable expected = new ImmutableBytesWritable("100.75.230.9".getBytes(), true);
		final byte[] buffer = ByteUtils.loadBytes("0x644be609");
		ImmutableBytesWritable bytesWritable = new ImmutableBytesWritable(buffer, true);
		ImmutableBytesWritable actual = evaluate(bytesWritable);
		assertEquals(expected, actual);
	}

	@Test
	public void testUdf_bytesToIPV6_ok() throws HiveException {
		// 检查ipv6
		final ImmutableBytesWritable expected = new ImmutableBytesWritable("2409:800b:5003:1705:0000:0000:0000:0101".getBytes(), true);
		final byte[] buffer = ByteUtils.loadBytes("0x2409800b500317050000000000000101");
		ImmutableBytesWritable bytesWritable = new ImmutableBytesWritable(buffer, true);
		ImmutableBytesWritable actual = evaluate(bytesWritable);
		assertEquals(expected, actual);
	}
}
