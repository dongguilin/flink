package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A column derived from an expression.
 */
public class SqlComputedColumn extends SqlTableColumn {

	private final SqlNode expr;

	public SqlComputedColumn(SqlParserPos pos, SqlIdentifier name, @Nullable SqlCharStringLiteral comment, SqlNode expr) {
		super(name, null, comment, pos);
		this.expr = requireNonNull(expr, "Column expression should not be null");
	}

	public SqlNode getExpr() {
		return expr;
	}

	@Override
	protected void unparseColumn(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.print(" ");
		writer.keyword("AS");
		expr.unparse(writer, leftPrec, rightPrec);
	}

	@Override
	public @Nonnull
	List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(name, expr, comment);
	}
}
