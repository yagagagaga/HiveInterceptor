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

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UDFBytesToStringTest {

	private UDFBytesToString udf;

	@Before
	public void setUp() throws HiveException {
		udf = new UDFBytesToString();
		ObjectInspector valueOI = ImmutableBinaryObjectInspector.ImmutableOI;
		ObjectInspector[] arguments = { valueOI };

		udf.initialize(arguments);
	}

	private byte[] evaluate(byte[] arg) throws HiveException {
		ImmutableBytesWritable output = evaluate(new ImmutableBytesWritable(arg, false));
		return output.copyBytes();
	}

	@SuppressWarnings("unchecked")
	private <T> T evaluate(Writable value) throws HiveException {
		DeferredObject valueObj = new DeferredJavaObject(value);
		DeferredObject[] args = { valueObj };
		return (T) udf.evaluate(args);
	}

	@Test
	public void testUdf_normalValue_ok() throws HiveException {
		final byte[] actual = evaluate("hello".getBytes());
		Assert.assertArrayEquals("hello".getBytes(), actual);
	}

	@Test
	public void testUdf_abnormalValue_ok() throws HiveException {
		byte[] expect = new byte[0];

		byte[] actual = evaluate(new byte[] { 0, 0, 0, 0 });
		Assert.assertArrayEquals(expect, actual);

		actual = evaluate(new byte[] { -1, -1, -1, -1 });
		Assert.assertArrayEquals(expect, actual);

		actual = evaluate(new byte[0]);
		Assert.assertArrayEquals(expect, actual);
	}

	@Test
	public void testUdf_errorValue_fail() {
		try {
			evaluate(new IntWritable(1));
			Assert.fail();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof ClassCastException);
		}
	}
}
