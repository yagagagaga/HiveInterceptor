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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.util.SM4New;

public class UDFSM4Encrypt extends GenericUDF {
	private final SM4New sm4 = new SM4New();

	private final Text txtResult = new Text();
	private final ImmutableBytesWritable bytesResult = new ImmutableBytesWritable(true);

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"date_sub() requires 2 argument, got " + arguments.length);
		}
		if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type arguments are accepted but "
							+ arguments[0].getTypeName() + " is passed. as first arguments");
		}
		if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but "
							+ arguments[1].getTypeName() + " is passed. as second arguments");
		}

		return arguments[0];
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		if (deferredObjects[0] == null || deferredObjects[0].get() == null) {
			return null;
		}

		if (deferredObjects[0].get() instanceof ImmutableBytesWritable) {
			return handleBinary(deferredObjects[0], deferredObjects[1]);
		} else {
			return handleText(deferredObjects[0], deferredObjects[1]);
		}
	}

	private Object handleText(DeferredObject data, DeferredObject key) throws HiveException {
		final byte[] input = javaStringObjectInspector.getPrimitiveJavaObject(data.get()).getBytes();
		final byte[] keys = javaStringObjectInspector.getPrimitiveJavaObject(key.get()).getBytes();

		if (input.length == 0 || keys.length == 0) {
			txtResult.set("");
		} else {
			byte[] output = new byte[512];
			txtResult.set(output, 0, sm4.encrypt(input, 0, input.length, keys, output));
		}
		return txtResult;
	}

	private Object handleBinary(DeferredObject data, DeferredObject key) throws HiveException {
		final ImmutableBytesWritable input = ImmutableOI.getPrimitiveWritableObject(data.get());
		final byte[] keys = javaStringObjectInspector.getPrimitiveJavaObject(key.get()).getBytes();
		if (input.getLength() == 0 || keys.length == 0) {
			bytesResult.set(new byte[0]);
		} else {
			byte[] output = new byte[512];
			bytesResult.set(output, 0,
					sm4.encrypt(input.get(), input.getOffset(), input.getLength(), keys, output));
		}
		return bytesResult;
	}

	@Override
	public String getDisplayString(String[] children) {
		return "sm4decrypt";
	}
}
