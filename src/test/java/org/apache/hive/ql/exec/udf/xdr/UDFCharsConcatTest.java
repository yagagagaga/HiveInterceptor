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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.Test;

public class UDFCharsConcatTest extends UDFBaseTest {

	@Override
	public GenericUDF createUDF() {
		return new UDFCharsConcat();
	}

	@Override
	public ObjectInspector[] argumentsOIs() {
		return new ObjectInspector[] { ImmutableOI, ImmutableOI };
	}

	@Test
	public void testUdf_normalValue_ok() throws HiveException {
		byte[] data = "1|2|3|4|5|6".getBytes();
		ImmutableBytesWritable first = new ImmutableBytesWritable(data, 2, 1, true);
		ImmutableBytesWritable second = new ImmutableBytesWritable(data, 8, 1, true);

		ImmutableBytesWritable actual = evaluate(first, second);
		assertArrayEquals("2|3|4|5".getBytes(), actual.copyBytes());
	}
}
