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

import static org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector.ImmutableOI;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class UDFNormalizeTest extends UDFBaseTest {

	@Override
	public GenericUDF createUDF() {
		return new UDFNormalize();
	}

	@Override
	public ObjectInspector[] argumentsOIs() {
		return new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.javaStringObjectInspector };
	}

	@Test
	public void testUdf_normalTxtValue_ok() throws HiveException {
		final Text expected = new Text("16914568569");

		Text actual = evaluate("16914568569");
		assertEquals(expected, actual);

		actual = evaluate("8616914568569");
		assertEquals(expected, actual);

		actual = evaluate("+8616914568569");
		assertEquals(expected, actual);

		actual = evaluate("08616914568569");
		assertEquals(expected, actual);

		actual = evaluate("008616914568569");
		assertEquals(expected, actual);
	}

	@Test
	public void testUdf_normalBinaryValue_ok() throws HiveException {
		udf.initialize(new ObjectInspector[] { ImmutableOI });

		final ImmutableBytesWritable expected = new ImmutableBytesWritable("16914568569".getBytes(), true);

		ImmutableBytesWritable actual = evaluate(new ImmutableBytesWritable("16914568569".getBytes(), true));
		assertEquals(expected, actual);

		actual = evaluate(new ImmutableBytesWritable("8616914568569".getBytes(), true));
		assertEquals(expected, actual);

		actual = evaluate(new ImmutableBytesWritable("+8616914568569".getBytes(), true));
		assertEquals(expected, actual);

		actual = evaluate(new ImmutableBytesWritable("08616914568569".getBytes(), true));
		assertEquals(expected, actual);

		actual = evaluate(new ImmutableBytesWritable("008616914568569".getBytes(), true));
		assertEquals(expected, actual);
	}

	@Test
	public void testUdf_abnormalTxtValue_ok() throws HiveException {
		Text actual = evaluate("");
		assertEquals("", actual.toString());

		actual = evaluate("dsfakewk");
		assertEquals("dsfakewk", actual.toString());

		actual = evaluate((Object) null);
		assertNull(actual);
	}

	@Test
	public void testUdf_abnormalBinaryValue_ok() throws HiveException {
		udf.initialize(new ObjectInspector[] { ImmutableOI });

		ImmutableBytesWritable actual = evaluate(new ImmutableBytesWritable(new byte[0], true));
		assertArrayEquals(new byte[0], actual.get());

		actual = evaluate(new ImmutableBytesWritable("dsfakewk".getBytes(), true));
		assertArrayEquals("dsfakewk".getBytes(), actual.get());

		actual = evaluate((Object) null);
		assertNull(actual);
	}
}
