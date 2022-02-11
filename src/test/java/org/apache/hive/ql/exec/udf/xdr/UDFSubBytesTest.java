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
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;
import static org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector.ImmutableOI;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class UDFSubBytesTest extends UDFBaseTest {

	@Override
	public GenericUDF createUDF() {
		return new UDFSubBytes();
	}

	@Override
	public ObjectInspector[] argumentsOIs() {
		return new ObjectInspector[] {
				javaStringObjectInspector, writableIntObjectInspector, writableIntObjectInspector };
	}

	@Test
	public void testUdf_normalTextValue_ok() throws HiveException {
		Text actual = evaluate("12345678910", 0, 7);
		assertEquals("1234567", actual.toString());
	}

	@Test
	public void testUdf_normalBinaryValue_ok() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				ImmutableOI, writableIntObjectInspector, writableIntObjectInspector });

		final byte[] expected = "1234567".getBytes();
		ImmutableBytesWritable actual = evaluate(
				new ImmutableBytesWritable("12345678910".getBytes(), true), 0, 7);

		assertArrayEquals(expected, actual.copyBytes());
	}

	@Test(expected = NullPointerException.class)
	public void testUdf_abnormalTextValue_ok() throws HiveException {
		Text actual = evaluate(null, 0, 7);
		assertNull(actual);

		final String value = "19999999999";

		actual = evaluate(value, 0, 10000);
		assertNull(actual);

		actual = evaluate(value, 0, -1);
		assertNull(actual);

		evaluate(value, null, 1);
		fail();
	}

	@Test(expected = NullPointerException.class)
	public void testUdf_abnormalBinaryValue_ok() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				ImmutableOI, writableIntObjectInspector, writableIntObjectInspector });

		Text actual = evaluate(null, 0, 7);
		assertNull(actual);

		final ImmutableBytesWritable value = new ImmutableBytesWritable("12345678910".getBytes(), true);

		actual = evaluate(value, 0, 10000);
		assertNull(actual);

		actual = evaluate(value, 0, -1);
		assertNull(actual);

		evaluate(value, null, 1);
		fail();
	}
}
