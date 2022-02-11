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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;

public class UDFBytesConcat extends GenericUDF {
	private final ImmutableBinaryObjectInspector binaryOI = ImmutableBinaryObjectInspector.ImmutableOI;
	private final ImmutableBytesWritable result = new ImmutableBytesWritable(false);

	@Override
	public String getDisplayString(String[] children) {
		return "bytesconcat(" + String.join(",", children) + ")";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors) {
		return ImmutableBinaryObjectInspector.ImmutableOI;
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		ImmutableBytesWritable first = binaryOI.getPrimitiveWritableObject(deferredObjects[0].get());
		ImmutableBytesWritable second = binaryOI.getPrimitiveWritableObject(deferredObjects[1].get());

		int offset = first.getOffset();
		int length = second.getOffset() - first.getOffset() + second.getLength();
		result.set(first.get(), offset, length);
		return result;
	}
}
