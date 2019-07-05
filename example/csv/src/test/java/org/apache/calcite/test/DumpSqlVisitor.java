package org.apache.calcite.test;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlShuttle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumpSqlVisitor extends SqlShuttle {

    private static final Logger logger = LoggerFactory.getLogger(DumpSqlVisitor.class);

    @Override
    public SqlNode visit(SqlLiteral literal) {
        logger.debug("找到 字面值类型[{}], 值为 {}", literal.getKind(), literal.toString());
        return super.visit(literal);
    }

    @Override
    public SqlNode visit(SqlCall call) {
        logger.debug("找到 调用类型[{}], 值为 {}", call.getKind(), call.toString());
        return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
        logger.debug("找到 列表 {}", nodeList.toString());
        return super.visit(nodeList);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        logger.debug("找到 变量类型为[{}], 值为 {}", id.getKind(), id.toString());
        return super.visit(id);
    }
}
