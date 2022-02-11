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

public class UDFSM4EncryptTest extends UDFBaseTest {

	@Override
	public GenericUDF createUDF() {
		return new UDFSM4Encrypt();
	}

	@Override
	public ObjectInspector[] argumentsOIs() {
		return new ObjectInspector[] { javaStringObjectInspector, javaStringObjectInspector };
	}

	@Test
	public void testUdf_normalTextValue_ok() throws HiveException {
		Text actual = evaluate("19999999999", "hNLJj#@FgPQYygQA");
		assertEquals(new Text("8392835f5fe1432f253792adf22fc6b8"), actual);
	}

	@Test
	public void testUdf_normalBinaryValue_ok() throws HiveException {
		udf.initialize(new ObjectInspector[] { ImmutableOI, ImmutableOI });

		final byte[] expected = "8392835f5fe1432f253792adf22fc6b8".getBytes();
		ImmutableBytesWritable actual = evaluate(
				new ImmutableBytesWritable("19999999999".getBytes(), true),
				new Text("hNLJj#@FgPQYygQA"));

		assertArrayEquals(expected, actual.copyBytes());
	}

	@Test
	public void testUdf_abnormalTextValue_ok() throws HiveException {
		Text actual = evaluate(null, null);
		assertNull(actual);

		actual = evaluate("", "");
		assertArrayEquals(new byte[0], actual.copyBytes());
	}

	@Test
	public void testUdf_abnormalBinaryValue_ok() throws HiveException {
		udf.initialize(new ObjectInspector[] { ImmutableOI, ImmutableOI });

		ImmutableBytesWritable actual = evaluate(null, null);
		assertNull(actual);

		actual = evaluate(
				new ImmutableBytesWritable("".getBytes(), true),
				new Text());
		assertArrayEquals(new byte[0], actual.copyBytes());
	}
}