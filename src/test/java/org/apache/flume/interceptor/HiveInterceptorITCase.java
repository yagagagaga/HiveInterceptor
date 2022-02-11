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
package org.apache.flume.interceptor;

import static org.apache.flume.interceptor.HiveInterceptorConstants.INPUT_COLUMN_DELIMITER;
import static org.apache.flume.interceptor.HiveInterceptorConstants.INPUT_COLUMN_LENGTHS;
import static org.apache.flume.interceptor.HiveInterceptorConstants.INPUT_COLUMN_NUM;
import static org.apache.flume.interceptor.HiveInterceptorConstants.SQL;
import static org.junit.Assert.assertEquals;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.util.ByteUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class HiveInterceptorITCase {
	private static Interceptor.Builder builder;

	@BeforeClass
	public static void setUp() throws Exception {
		builder = InterceptorBuilderFactory.newInstance(HiveInterceptorBuilder.class.getName());
	}

	@Test
	public void test_data() {
		Context ctx = new Context();
		String sql = ""
				+ "select c1,tbcd(c2),tbcd(c3),tbcd(c4),tbcd(c5),c6,c7,c8,c9,tbcd(c10),tbcd(c11),"
				+ "       normalize(tbcd(c12)),c13,bytestotimestamp(c14),bytestotimestamp(c15),"
				+ "       bytestodouble(c16),bytestodouble(c17),c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,"
				+ "       c28,c29,c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,c40,c41,c42,c43,c44,c45,"
				+ "       bytestoip(c46),bytestoip(c47),bytestoip(c48),bytestoip(c49),bytestoip(c50),"
				+ "       c51,c52,c53,c54,c55,c56,c57,c58,cuttail(normalize(tbcd(c12)),4)"
				+ "from event "
				+ "where c1 is not null";
		ctx.put(SQL, sql);
		ctx.put(INPUT_COLUMN_LENGTHS, "2,2,2,2,2,1,1,16,1,8,8,16,1,8,8,8,8,2,1,1,1,1,1,1,1,1,1,1,1,1,"
				+ "1,1,1,1,1,1,1,4,2,1,4,2,1,4,4,4,16,16,16,16,2,2,2,4,2,4,32,1+[16]*N");

		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		interceptor.initialize();

		String mme = ""
				+ "0x00f173f13049ffffffffff0500000000000000006168d9b1ccec555d06ffffffffffffffffffffffffffff"
				+ "ffffffffffffffffffffffffffffffffffff140000017c81925b920000017c81925bd8ffffffffffffffffff"
				+ "ffffffffffffffffffff07010114ffff01ffffffffffffffffffffff17500567ffffffffffffff04082cdd6e"
				+ "6bc1ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0a232b81ffff"
				+ "ffffffffffffffffffff6461eca8ffffffffffffffffffffffff644be6098e3c8e3c496c04187a84ffffffff"
				+ "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff01050108050114ffffff"
				+ "ffffffffffffff";

		Event event = EventBuilder.withBody(ByteUtils.loadBytes(mme));
		event = interceptor.intercept(event);

		String expected = ""
				+ "241|371|0394||||5|0x00000000000000006168d9b1ccec555d|6||||20|1634261425042|"
				+ "1634261425112|||||7|1|1|20|||1||||||||||||391120231||||1032|44|3715001281||||"
				+ "10.35.43.129|100.97.236.168|100.75.230.9|36412|36412|18796|68713092||||"
				+ "0x01050108050114ffffffffffffffffffff|";

		assertEquals(expected, new String(event.getBody()));
	}
}
