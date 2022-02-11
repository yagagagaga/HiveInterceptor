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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;

public abstract class UDFBaseTest {
	protected GenericUDF udf;

	public abstract GenericUDF createUDF();

	public abstract ObjectInspector[] argumentsOIs();

	@Before
	public void setUp() throws HiveException {
		udf = createUDF();
		udf.initialize(argumentsOIs());
	}

	@SuppressWarnings("unchecked")
	protected <T> T evaluate(Object... values) throws HiveException {
		DeferredObject[] args = new DeferredObject[values.length];

		for (int i = 0; i < values.length; i++) {
			args[i] = new DeferredJavaObject(box(values[i]));
		}

		return (T) udf.evaluate(args);
	}

	private Writable box(Object value) {

		if (value == null) {
			return null;
		}

		String className = value.getClass().getName();

		switch (className) {
		case "byte":
		case "java.lang.Byte":
			return new ByteWritable(((Byte) value));
		case "short":
		case "java.lang.Short":
			return new ShortWritable((Short) value);
		case "int":
		case "java.lang.Integer":
			return new IntWritable(((Integer) value));
		case "long":
		case "java.lang.Long":
			return new LongWritable(((Long) value));
		case "float":
		case "java.lang.Float":
			return new FloatWritable(((Float) value));
		case "double":
		case "java.lang.Double":
			return new DoubleWritable(((Double) value));
		case "boolean":
		case "java.lang.Boolean":
			return new BooleanWritable(((Boolean) value));
		case "java.lang.String":
			return new Text(((String) value));
		default:
			if (Writable.class.isAssignableFrom(value.getClass())) {
				return ((Writable) value);
			} else {
				return null;
			}
		}
	}

	// TestItemBuilder.testItem("description")
	// .input()
	// .expect()

}
