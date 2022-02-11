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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector.ImmutableOI;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class UDFNormalizeTimestampTest extends UDFBaseTest {

	@Override
	public GenericUDF createUDF() {
		return new UDFNormalizeTimestamp();
	}

	@Override
	public ObjectInspector[] argumentsOIs() {
		return new ObjectInspector[] { javaStringObjectInspector };
	}

	@Test
	public void testUdf_normalTextValue_ok() throws HiveException {
		Text actual = evaluate("20211011 11:36:15:456");
		assertEquals(new Text("2021-10-11 11:36:15.456"), actual);

		actual = evaluate("2021-10-11 11:36:15.456");
		assertEquals(new Text("2021-10-11 11:36:15.456"), actual);
	}

	@Test
	public void testUdf_normalBinaryValue_ok() throws HiveException {
		udf.initialize(new ObjectInspector[] { ImmutableOI });

		final byte[] expected = "2021-10-11 11:36:15.456".getBytes();
		ImmutableBytesWritable actual = evaluate(new ImmutableBytesWritable("20211011 11:36:15:456".getBytes(), true));
		assertArrayEquals(expected, actual.copyBytes());

		actual = evaluate(new ImmutableBytesWritable("2021-10-11 11:36:15.456".getBytes(), true));
		assertArrayEquals(expected, actual.copyBytes());
	}

	@Test
	public void testUdf_abnormalTextValue_ok() throws HiveException {
		Text actual = evaluate((Object) null);
		assertNull(actual);

		actual = evaluate("");
		assertNull(actual);
	}

	@Test
	public void testUdf_abnormalBinaryValue_ok() throws HiveException {
		udf.initialize(new ObjectInspector[] { ImmutableOI });

		ImmutableBytesWritable actual = evaluate((Object) null);
		assertNull(actual);

		actual = evaluate(new ImmutableBytesWritable("".getBytes(), true));
		assertArrayEquals(new byte[0], actual.copyBytes());
	}
}