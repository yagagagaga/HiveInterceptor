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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hive.serde2.DelimitedByteSerDe;
import org.apache.hive.serde2.FlumeEventSerDe;
import org.apache.hive.serde2.TlvByteSerDe;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

public class HiveProcessorFactory {

	private static final Logger LOG = LoggerFactory.getLogger(
			Thread.currentThread().getStackTrace()[0].getClassName());
	private static final Map<String, String> TABLE_CACHE = new ConcurrentHashMap<>();

	public static void main(String[] args) {
		final Driver driver = new Driver(Support.hiveConf);
		Scanner scanner = new Scanner(System.in);
		while (scanner.hasNextLine()) {
			final String cmd = scanner.nextLine();
			try {
				driver.run(cmd);
				final LinkedList<Object> res = new LinkedList<>();
				driver.getResults(res);
				res.forEach(System.out::println);
			} catch (CommandNeedRetryException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	public AbstractSerDe createSerDeTlv(String inputColumnLengths) {
		final String tableName = TABLE_CACHE.computeIfAbsent(
				inputColumnLengths, s -> Support.createTableTlv(inputColumnLengths));
		try {
			return (AbstractSerDe) MetaStoreUtils
					.getDeserializer(Support.hiveConf, Support.db.getTable(tableName).getTTable());
		} catch (MetaException | HiveException e) {
			throw Throwables.propagate(e);
		}
	}

	public AbstractSerDe createSerDe(String inputColumnLengths) {
		final String tableName = TABLE_CACHE.computeIfAbsent(
				inputColumnLengths, s -> Support.createTable(inputColumnLengths));
		try {
			return (AbstractSerDe) MetaStoreUtils
					.getDeserializer(Support.hiveConf, Support.db.getTable(tableName).getTTable());
		} catch (MetaException | HiveException e) {
			throw Throwables.propagate(e);
		}
	}

	public AbstractSerDe createSerDe(int inputColumnNum,
			String inputColumnDelimiter) {
		final String tableName = TABLE_CACHE.computeIfAbsent(
				inputColumnNum + " " + inputColumnDelimiter,
				s -> Support.createTable(inputColumnNum, inputColumnDelimiter));
		try {
			return (AbstractSerDe) MetaStoreUtils
					.getDeserializer(Support.hiveConf, Support.db.getTable(tableName).getTTable());
		} catch (MetaException | HiveException e) {
			throw Throwables.propagate(e);
		}
	}

	public <R> HiveProcessor<R> createProcessorTlv(String inputColumnLengths, String sqlTemplate,
			Function<Object[], R> resultConsumer) {
		final String tableName = TABLE_CACHE.computeIfAbsent(
				inputColumnLengths, s -> Support.createTableTlv(inputColumnLengths));

		final String sql = sqlTemplate.replaceAll("event", tableName);
		return new HiveProcessor<>(Support.hiveConf, sql, resultConsumer);
	}

	public <R> HiveProcessor<R> createProcessor(String inputColumnLengths, String sqlTemplate,
			Function<Object[], R> resultConsumer) {
		final String tableName = TABLE_CACHE.computeIfAbsent(
				inputColumnLengths, s -> Support.createTable(inputColumnLengths));

		final String sql = sqlTemplate.replaceAll("event", tableName);
		return new HiveProcessor<>(Support.hiveConf, sql, resultConsumer);
	}

	public <R> HiveProcessor<R> createProcessor(int inputColumnNum,
			String inputColumnDelimiter,
			String sqlTemplate,
			Function<Object[], R> resultConsumer) {
		final String tableName = TABLE_CACHE.computeIfAbsent(
				inputColumnNum + " " + inputColumnDelimiter,
				s -> Support.createTable(inputColumnNum, inputColumnDelimiter));

		final String sql = sqlTemplate.replaceAll("event", tableName);
		return new HiveProcessor<>(Support.hiveConf, sql, resultConsumer);
	}

	private static class Support {
		private static final AtomicInteger TABLE_ID = new AtomicInteger(0);
		private static Hive db;
		private static HiveConf hiveConf;

		static {
			try {
				lockShimVersion();
				createWorkingDirectory();
				createHiveDB();
				registerUDFs();
			} catch (Exception e) {
				throw Throwables.propagate(e);
			}
		}

		private static void lockShimVersion() throws NotFoundException, CannotCompileException {
			ClassPool classPool = ClassPool.getDefault();
			CtClass shimLoaderClass = classPool.get("org.apache.hadoop.hive.shims.ShimLoader");
			shimLoaderClass.getDeclaredMethod("getMajorVersion").setBody("return \"0.23\";");
			shimLoaderClass.toClass();
		}

		@SuppressWarnings("ResultOfMethodCallIgnored")
		private static void createWorkingDirectory() throws IOException {
			String warehouseDir = System.getProperty("flume.warehouse.dir", "./data");
			File warehouse = new File(warehouseDir);
			FileUtils.forceMkdir(warehouse);
			System.setProperty("flume.warehouse.dir", warehouse.getAbsolutePath());

			String scratchDir = System.getProperty("flume.scratch.dir", "./tmp");
			File scratch = new File(scratchDir);
			FileUtils.forceMkdir(scratch);
			scratch.setWritable(true, false);
			scratch.setReadable(true, false);
			scratch.setExecutable(true, false);
			System.setProperty("flume.scratch.dir", scratch.getAbsolutePath());
		}

		private static void createHiveDB() throws HiveException {
			String dbName = "flume_" + getProcessId();
			String dbConnect = String.format("jdbc:derby:;databaseName=%s;create=true", dbName);

			Configuration conf = new Configuration(false);
			conf.set("fs.defaultFS", "file:///");

			hiveConf = new HiveConf(conf, HiveProcessorFactory.class);
			hiveConf
					.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, System.getProperty("flume.warehouse.dir"));
			hiveConf.setVar(HiveConf.ConfVars.SCRATCHDIR, System.getProperty("flume.scratch.dir"));
			hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, dbConnect);
			hiveConf.setBoolVar(HiveConf.ConfVars.HIVEEXPREVALUATIONCACHE, false);

			SessionState sessionState = SessionState.get();

			if (sessionState == null) {
				SessionState.start(hiveConf);
			}

			db = Objects.requireNonNull(Hive.get(hiveConf), "create hive database fail");
		}

		/**
		 * 通过反射，把所有 udf 注册到 hive 里面
		 */
		private static void registerUDFs() {

			Reflections reflections = new Reflections("org.apache.hive.ql.exec.udf.xdr");

			for (Class<?> info : reflections.getSubTypesOf(GenericUDF.class)) {
				String className = info.getSimpleName();
				String functionName = className.substring("UDF".length()).toLowerCase();
				LOG.info("register function:" + functionName);

				FunctionRegistry.registerTemporaryFunction(functionName, info);
			}
		}

		private static String getProcessId() {
			String name = ManagementFactory.getRuntimeMXBean().getName();
			return name.split("@")[0];
		}

		private static String getType(String typeChar) {
			switch (typeChar) {
			case "s":
				return serdeConstants.STRING_TYPE_NAME;
			case "b":
				return serdeConstants.BIGINT_TYPE_NAME;
			case "i":
				return serdeConstants.INT_TYPE_NAME;
			default:
				throw new IllegalArgumentException("invalid type " + typeChar);
			}
		}

		/**
		 * 根据（TLV）字段个数创建 hive 表
		 *
		 * @param columnLengths 每字段的字节长度，用逗号分隔开来
		 * @return hive 表名
		 */
		private static String createTableTlv(String columnLengths) {
			int columnNum = columnLengths.split(",").length;

			return createTable(columnNum, serdeConstants.BINARY_TYPE_NAME, table -> {
				table.setSerializationLib(TlvByteSerDe.class.getName());
				table.setSerdeParam(serdeConstants.FIELD_DELIM, ",");
				table.setSerdeParam(TlvByteSerDe.TLV_COLUMNS, columnLengths);
			});
		}

		/**
		 * 根据用户指定的配置创建 hive 表
		 *
		 * @param columnNum        字段个数
		 * @param columnType       字段类型
		 * @param tableSerDeSetter 创建 Hive Table 时，指定的 SerDe 参数
		 * @return hive 表名
		 */
		private static String createTable(int columnNum, String columnType,
				Consumer<Table> tableSerDeSetter) {
			SessionState sessionState = SessionState.get();

			if (sessionState == null) {
				SessionState.start(hiveConf);
			}

			int tableIndex = TABLE_ID.getAndIncrement();
			String tableName = "flume_" + tableIndex;

			Table table = new Table("default", tableName);
			List<FieldSchema> fields = table.getCols();

			for (int i = 1; i <= columnNum; ++i) {
				fields.add(new FieldSchema("c" + i, columnType, null));
			}

			tableSerDeSetter.accept(table);

			try {
				db.createTable(table);
			} catch (Exception e) {
				LOG.error("cannot create hive table", e);
				throw new RuntimeException(e.getMessage(), e);
			}

			return tableName;
		}

		/**
		 * 根据每个字段的字节长度创建 hive 表
		 *
		 * @param columnLengths 每字段的字节长度，用逗号分隔开来
		 * @return hive 表名
		 */
		private static String createTable(String columnLengths) {
			int columnNum = columnLengths.split(",").length;

			return createTable(columnNum, serdeConstants.BINARY_TYPE_NAME, table -> {
				table.setSerializationLib(DelimitedByteSerDe.class.getName());
				table.setSerdeParam(serdeConstants.FIELD_DELIM, ",");
				table.setSerdeParam(DelimitedByteSerDe.COLUMN_LENGTHS, columnLengths);
			});
		}

		/**
		 * 根据分隔符和字段个数创建 hive 表
		 *
		 * @param columms   字段个数
		 * @param separator 分隔符
		 * @return hive 表名
		 */
		private static String createTable(int columms, String separator) {
			return createTable(columms, serdeConstants.STRING_TYPE_NAME, table -> {
				table.setSerializationLib(OpenCSVSerde.class.getName());
				table.setSerdeParam(serdeConstants.FIELD_DELIM, separator);
				table.setSerdeParam(OpenCSVSerde.SEPARATORCHAR, separator);
			});
		}

		/**
		 * 根据字段类型和分隔符创建表
		 *
		 * @param columnTypes 字段类型，用<tt>:</tt>号拼接
		 * @param fieldDelim  字段分隔符
		 * @deprecated 由于此方法暂时未用到，所以标为弃用
		 */
		@Deprecated
		private static void createTable(String columnTypes, String fieldDelim) {
			SessionState sessionState = SessionState.get();

			if (sessionState == null) {
				SessionState.start(hiveConf);
			}

			int tableIndex = TABLE_ID.getAndIncrement();
			String tableName = "flume_" + tableIndex;

			Table table = new Table("default", tableName);
			String[] types = columnTypes.split(":");
			List<FieldSchema> fields = table.getCols();

			for (int i = 1; i <= types.length; ++i) {
				String type = getType(types[i - 1]);
				fields.add(new FieldSchema("c" + i, type, null));
			}
			fields.add(new FieldSchema("h", "map<string,string>", null));

			table.setSerdeParam(serdeConstants.FIELD_DELIM, fieldDelim);
			table.setSerializationLib(FlumeEventSerDe.class.getName());

			try {
				db.createTable(table);
			} catch (Exception e) {
				LOG.error("cannot create hive table", e);
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}
}
