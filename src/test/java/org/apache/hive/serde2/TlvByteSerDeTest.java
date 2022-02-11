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
package org.apache.hive.serde2;

import static org.junit.Assert.assertArrayEquals;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.ByteBufWritable;
import org.apache.io.Output;
import org.apache.util.TlvUtils;
import org.junit.Test;

public class TlvByteSerDeTest {
	private final Properties props = new Properties();
	private final TlvByteSerDe serDe = new TlvByteSerDe();

	@Test
	public void testDeserialize() throws Exception {
		props.setProperty(TlvByteSerDe.TLV_COLUMNS, "tlv20,tlv41");

		serDe.initialize(new Configuration(), props);

		final Output output = new Output(128);
		output.write(TlvUtils.tlv((short) 20, "a".getBytes()));
		output.write(TlvUtils.tv((short) 41, "b".getBytes()));
		final ByteBufWritable input = new ByteBufWritable(output.copyData());
		final ImmutableBytesWritable[] row = (ImmutableBytesWritable[]) serDe.deserialize(input);

		assertArrayEquals("a".getBytes(), row[0].copyBytes());
		assertArrayEquals("b".getBytes(), row[1].copyBytes());
	}
}
