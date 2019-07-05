/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.csv;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table based on a CSV file that can implement simple filtering.
 *
 * <p>It implements the {@link FilterableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext, List)} method.
 */
public class CsvFilterableTable extends CsvTable
        implements FilterableTable {

    private static final Logger logger = LoggerFactory.getLogger(CsvFilterableTable.class);

    /**
     * Creates a CsvFilterableTable.
     */
    public CsvFilterableTable(Source source, RelProtoDataType protoRowType) {
        super(source, protoRowType);
    }

    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        final String[] filterValues = new String[fieldTypes.size()];
        filters.removeIf(filter -> addFilter(filter, filterValues));
        final int[] fields = CsvEnumerator.identityList(fieldTypes.size());

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            int tmp = 0;
            for (String val : filterValues) {
                sb.append(tmp).append("->").append(val);
            }
            logger.debug(sb.toString());
        }
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>(source, cancelFlag, false, filterValues,
                        new CsvEnumerator.ArrayRowConverter(fieldTypes, fields));
            }
        };
    }

    /**
     * @param filter       过滤表达式
     * @param filterValues 用于缓存过滤值
     * @return 保留该过滤条件则返回true
     */
    private boolean addFilter(RexNode filter, Object[] filterValues) {
        // 这个分支的含义不理解
        if (filter.isA(SqlKind.AND)) {
            // We cannot refine(remove) the operands of AND,
            // it will cause o.a.c.i.TableScanNode.createFilterable filters check failed.
            ((RexCall) filter).getOperands().forEach(subFilter -> addFilter(subFilter, filterValues));
        } else if (filter.isA(SqlKind.EQUALS)) {
            final RexCall call = (RexCall) filter;

            // 取出左操作数
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }

            // 取出右操作数
            final RexNode right = call.getOperands().get(1);

            // 若符合"变量名 = 字面值"的Pattern,
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                logger.debug("找到需要过滤的元素 {}", right);
                // 将过滤值放到需要的位置
                final int index = ((RexInputRef) left).getIndex();
                if (filterValues[index] == null) {
                    filterValues[index] = ((RexLiteral) right).getValue2().toString();
                    return true;
                }
            }
        }
        return false;
    }

    public String toString() {
        return "CsvFilterableTable";
    }
}