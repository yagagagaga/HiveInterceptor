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

import java.math.BigInteger;
import java.security.DigestException;
import java.security.MessageDigest;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

public class UDFMd5 extends GenericUDF {

	private final MessageDigest messageDigest;
	private final ImmutableBytesWritable byteResult = new ImmutableBytesWritable(new byte[16], false);
	private final Text txtResult = new Text(new byte[16]);

	public UDFMd5() {
		try {
			messageDigest = MessageDigest.getInstance("md5");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return "md5(" + String.join(",", children) + ")";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors)
			throws UDFArgumentLengthException {
		if (objectInspectors.length != 1 && objectInspectors.length != 2) {
			throw new UDFArgumentLengthException(
					"md5() requires at least 1 argument, got " + objectInspectors.length);
		}

		return objectInspectors[0];
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		if (deferredObjects[0] == null || deferredObjects[0].get() == null) {
			return null;
		}

		if (deferredObjects[0].get() instanceof ImmutableBytesWritable) {
			return handleBinary(deferredObjects[0], deferredObjects.length == 2 ? deferredObjects[1] : null);
		} else {
			return handleText(deferredObjects[0], deferredObjects.length == 2 ? deferredObjects[1] : null);
		}
	}

	private Object handleText(DeferredObject a, DeferredObject b) throws HiveException {
		byte[] str = javaStringObjectInspector.getPrimitiveJavaObject(a.get()).getBytes();

		messageDigest.update(str);
		if (b != null) {
			Text salt = javaStringObjectInspector.getPrimitiveWritableObject(b.get());
			messageDigest.update(salt.getBytes());
		}
		try {
			byte[] res = new byte[16];
			messageDigest.digest(res, 0, 16);
			txtResult.set(new BigInteger(1, res).toString(16));
			return txtResult;
		} catch (DigestException e) {
			throw new HiveException(e);
		}
	}

	private Object handleBinary(DeferredObject a, DeferredObject b) throws HiveException {
		ImmutableBytesWritable str = ImmutableOI.getPrimitiveWritableObject(a.get());

		messageDigest.update(str.get(), str.getOffset(), str.getLength());
		if (b != null) {
			Text salt = javaStringObjectInspector.getPrimitiveWritableObject(b.get());
			messageDigest.update(salt.getBytes());
		}
		try {
			messageDigest.digest(byteResult.get(), 0, 16);
			return byteResult;
		} catch (DigestException e) {
			throw new HiveException(e);
		}
	}
}
