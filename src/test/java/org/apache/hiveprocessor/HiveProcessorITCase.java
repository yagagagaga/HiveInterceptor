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
package org.apache.hiveprocessor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.ByteBufWritable;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("这个测试虽然能手动执行通过，但在 Maven 打包过程中不能通过")
public class HiveProcessorITCase {

	private static final List<Text> DATA = Stream.of(
			// 学号|姓名|班级|性别|语文成绩|数学成绩|英语成绩
			"1|陈文章|1|男|89|85|87",
			"2|张强|2|男|89|85|87",
			"3|李芬|2|女|89|85|87",
			"4|陆洋|3|男|89|85|87",
			"5|宋令文|3|男|89|85|87")
			.map(Text::new)
			.collect(Collectors.toList());

	private static HiveProcessorFactory factory;

	@BeforeClass
	public static void setUp() {
		factory = new HiveProcessorFactory();
	}

	@Test
	public void testText_normalScene_ok() throws SerDeException {

		int inputColumnNum = 6;
		String inputColumnDelimiter = "|";
		String sql = "select c1, c2 from event where c1 is not null";
		final HiveProcessor<Object[]> processor = factory
				.createProcessor(inputColumnNum, inputColumnDelimiter, sql, cols -> cols);
		final AbstractSerDe serDe = factory.createSerDe(inputColumnNum, inputColumnDelimiter);

		String data = "2021-09-21 18:29:25.366|2021-09-21 18:29:27.138|111|12|14|10770";
		final Object deserializeRow = serDe.deserialize(new Text(data));
		final Object[] cols = processor.process(deserializeRow);

		assertNotNull(cols);
		assertEquals(2, cols.length);

		assertTrue(cols[0] instanceof String);
		assertTrue(cols[1] instanceof String);

		assertEquals("2021-09-21 18:29:25.366", cols[0].toString());
		assertEquals("2021-09-21 18:29:27.138", cols[1].toString());
	}

	@Test
	public void testBinary_normalScene_ok() throws SerDeException {
		String inputColumnLengths = "1,2,8,8,14";
		String sql = "select c3, c4 from event where c1 is not null";
		final HiveProcessor<Object[]> processor = factory
				.createProcessor(inputColumnLengths, sql, x -> x);
		final AbstractSerDe serDe = factory.createSerDe(inputColumnLengths);

		// 2,245,1633676890280,1633676890280,abcdefghijklmn
		byte[] data = { 2, 0, -11, 0, 0, 1, 124, 94, -69, 16, -88, 0, 0, 1, 124, 94, -69, 16, -88, 97,
				98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110 };

		final Object deserializedRow = serDe.deserialize(new ByteBufWritable(data));
		final Object[] cols = processor.process(deserializedRow);
		assertNotNull(cols);
		assertEquals(2, cols.length);

		assertTrue(cols[0] instanceof ImmutableBytesWritable);
		assertTrue(cols[1] instanceof ImmutableBytesWritable);

		assertArrayEquals(new byte[] { 0, 0, 1, 124, 94, -69, 16, -88 },
				((ImmutableBytesWritable) cols[0]).copyBytes());
		assertArrayEquals(new byte[] { 0, 0, 1, 124, 94, -69, 16, -88 },
				((ImmutableBytesWritable) cols[1]).copyBytes());
	}

	private HiveProcessor<Object[]> prepareProcessor(String sql) {
		int inputColumnNum = 7;
		String inputColumnDelimiter = "|";

		return factory
				.createProcessor(inputColumnNum, inputColumnDelimiter, sql, x -> x);
	}

	private AbstractSerDe ceateSerDe() {
		return factory.createSerDe(7, "|");
	}

	@Test
	@Ignore("Not supported at the moment")
	public void testSql_groupBy_ok() throws SerDeException {
		final HiveProcessor<Object[]> processor = prepareProcessor(
				"select count(*) from event group by 1");
		final AbstractSerDe serDe = ceateSerDe();

		for (Text datum : DATA) {
			final Object deserializedRow = serDe.deserialize(datum);
			final Object[] cols = processor.process(deserializedRow);
			assertNotNull(cols);
			assertEquals(1, cols.length);
			assertEquals("5", cols[0].toString());
		}
	}

	@Test
	@Ignore("Not supported at the moment")
	public void testSql_orderBy_ok() throws SerDeException {
		final HiveProcessor<Object[]> processor = prepareProcessor(
				"select c1 from event order by c1 desc");
		final AbstractSerDe serDe = ceateSerDe();

		for (Text datum : DATA) {
			final Object deserializedRow = serDe.deserialize(datum);
			final Object[] cols = processor.process(deserializedRow);
			assertNotNull(cols);
			assertEquals(5, cols.length);
			int id = 1;
			for (Object actual : cols) {
				assertEquals(String.valueOf(id), actual.toString());
				id++;
			}
		}
	}

	@Test
	@Ignore("Not supported at the moment")
	public void testSql_select_ok() throws SerDeException {
		final HiveProcessor<Object[]> processor = prepareProcessor("select c2, c4 from event");
		final AbstractSerDe serDe = ceateSerDe();

		for (Text datum : DATA) {
			final Object deserializedRow = serDe.deserialize(datum);
			final Object[] cols = processor.process(deserializedRow);

			assertNotNull(cols);
			assertEquals(5, cols.length);
			List<String> expects = Arrays.asList(
					"陈文章|男",
					"张强|男",
					"李芬|女",
					"陆洋|男",
					"宋令文|男");
			for (int i = 0; i < expects.size(); i++) {
				assertEquals(expects.get(i), cols[i].toString());
			}
		}
	}

	@Test
	@Ignore("Not supported at the moment")
	public void testSql_where_ok() throws SerDeException {
		final HiveProcessor<Object[]> processor = prepareProcessor(
				"select c2 from event where c4 = '女'");
		final AbstractSerDe serDe = ceateSerDe();

		for (Text datum : DATA) {
			final Object deserializedRow = serDe.deserialize(datum);
			final Object[] cols = processor.process(deserializedRow);

			System.out.println(Arrays.toString(cols));
//			assertNotNull(cols);
//			assertEquals(1, cols.length);
//			assertEquals("李芬", cols[0].toString());
		}
	}

	@Test
	public void testSql_update_ok() {
	}

	@Test
	public void testSql_delete_ok() {
	}

	@Test
	public void testSql_insert_ok() {
	}

	@Test
	public void testSql_join_ok() {
	}
}
