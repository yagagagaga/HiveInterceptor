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
package org.apache.hive.ql.exec;

import java.util.function.Consumer;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class CustomSinkOperator extends Operator<ListSinkDesc> {

	private final StructObjectInspector oi;
	private final Consumer<Object[]> rowConsumer;

	public <T> CustomSinkOperator(StructObjectInspector oi, Consumer<Object[]> rowConsumer) {
		this.oi = oi;
		this.rowConsumer = rowConsumer;
	}

	@Override
	public void processOp(Object row, int tag) {
		final Object[] cols = oi.getStructFieldsDataAsList(row).toArray();
		rowConsumer.accept(cols);
	}

	@Override
	public OperatorType getType() {
		return OperatorType.FORWARD;
	}

	public Operator<? extends OperatorDesc> clone() {
		return new CustomSinkOperator(oi, rowConsumer);
	}

	public StructObjectInspector getOi() {
		return oi;
	}

	@Override
	public String getName() {
		return "CUSTOM_SINK";
	}
}
