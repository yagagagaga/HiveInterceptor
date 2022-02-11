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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hive.ql.exec.CustomSinkOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class HiveProcessor<R> {

	private static final Logger LOG = LoggerFactory.getLogger(
			Thread.currentThread().getStackTrace()[0].getClassName());
	private static final Map<String, Operator<?>> OPERATOR_CAHCE = new ConcurrentHashMap<>();

	private final ResultFetcher fetcher = new ResultFetcher();
	private final Operator<?> operator;
	private final Function<Object[], R> resultConsumer;

	public HiveProcessor(HiveConf hiveConf, String sql, Function<Object[], R> resultConsumer) {
		this.resultConsumer = resultConsumer;
		this.operator = complie(sql, fetcher, hiveConf);
	}

	private void explain(String sql, Driver driver) {
		try {
			driver.run("explain " + sql);

			List<?> res = new LinkedList<>();
			driver.getResults(res);
			res.forEach(System.out::println);

		} catch (CommandNeedRetryException | IOException e) {
			e.printStackTrace();
		}
	}

	private Operator<?> deepClone(Operator<?> op,
			ResultFetcher resultFetcher) throws CloneNotSupportedException {
		List<Operator<? extends OperatorDesc>> childOperators = op.getChildOperators();
		if (!childOperators.isEmpty()) {
			final Operator<? extends OperatorDesc> parent = op.clone();
			parent.getChildOperators().clear();
			parent.getParentOperators().clear();
			for (Operator<? extends OperatorDesc> operator : childOperators) {
				final Operator<?> child = deepClone(operator, resultFetcher);

				parent.getChildOperators().add(child);
				child.getParentOperators().add(parent);
			}
			return parent;
		}

		// It's ugly, but useful
		if (op instanceof CustomSinkOperator) {
			final CustomSinkOperator sink = (CustomSinkOperator) op;
			return new CustomSinkOperator(sink.getOi(), resultFetcher);
		}

		final Operator<? extends OperatorDesc> clone = op.clone();
		clone.getParentOperators().clear();
		clone.getChildOperators().clear();
		return clone;
	}

	private synchronized Operator<?> complie(String sql, ResultFetcher resultFetcher,
			HiveConf conf) {

		final Operator<?> cacheOperator = OPERATOR_CAHCE.get(sql);
		if (cacheOperator != null) {
			try {
				final Operator<?> deepClone = deepClone(cacheOperator, resultFetcher);
				deepClone.initialize(conf, cacheOperator.getInputObjInspectors());
				return deepClone;
			} catch (CloneNotSupportedException | HiveException e) {
				LOG.error(e.getMessage(), e);
				throw Throwables.propagate(e);
			}
		}

		SessionState sessionState = SessionState.get();

		if (sessionState == null) {
			SessionState.start(conf);
		}

		Driver driver = new Driver(conf);
		try {
			LOG.info("start to compile {}", sql);
			explain(sql, driver);

			driver.compile(sql);
			QueryPlan plan = Objects.requireNonNull(driver.getPlan(), "QueryPlan should not be null");
			FetchTask fetchTask = plan.getFetchTask();

			final Operator<?> tableScan = fetchTask.getWork().getSource();
			Operator<?> filter = tableScan.getChildOperators().get(0);
			filter.getParentOperators().clear();
			Operator<?> select;
			Operator<?> filterClone = filter.clone();
			Operator<?> selectClone;

			if (filter instanceof FilterOperator) {
				select = filter.getChildOperators().get(0);
				filterClone.getChildOperators().clear();
				selectClone = select.clone();
				filterClone.getChildOperators().add(selectClone);
				selectClone.getParentOperators().clear();
				selectClone.getParentOperators().add(filterClone);
			} else {
				select = filter;
				selectClone = filterClone;
			}

			ListSinkOperator listSink = (ListSinkOperator) (select.getChildOperators().get(0));
			listSink.getParentOperators().clear();

			CustomSinkOperator sink = new CustomSinkOperator(
					(StructObjectInspector) listSink.getInputObjInspectors()[0], resultFetcher);
			filterClone.getChildOperators().get(0).getChildOperators().clear();
			filterClone.getChildOperators().get(0).getChildOperators().add(sink);
			sink.getParentOperators().add(selectClone);

			filterClone.initialize(conf, filter.getInputObjInspectors());

			OPERATOR_CAHCE.put(sql, filterClone);
			return complie(sql, resultFetcher, conf);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw Throwables.propagate(e);
		} finally {
			driver.close();
		}
	}

	/**
	 * 使用 hive sql 对输入的数据进行处理
	 *
	 * @param deserializedRow hive 反序列化后的数据
	 * @return sql 处理后的结果
	 */
	@Nullable
	public R process(Object deserializedRow) {
		final Object[] columns = process(deserializedRow, operator, fetcher);
		return resultConsumer.apply(columns);
	}

	private Object[] process(Object row, Operator<?> operator, ResultFetcher fetcher) {
		try {
			operator.processOp(row, 0);
			return fetcher.result();
		} catch (HiveException e) {
			throw Throwables.propagate(e);
		}
	}

	private static class ResultFetcher implements Consumer<Object[]> {

		private Object[] result;
		private boolean accept;

		private Object[] result() {
			if (accept) {
				accept = false;
				return result;
			} else {
				return null;
			}
		}

		@Override
		public void accept(Object[] cols) {
			result = cols;
			accept = true;
		}
	}
}
