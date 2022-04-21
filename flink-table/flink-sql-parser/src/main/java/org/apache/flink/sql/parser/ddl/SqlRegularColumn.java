package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.type.ExtendedSqlType;

/**
 * A regular, physical column.
 */
public class SqlRegularColumn extends SqlTableColumn {

	public SqlRegularColumn(SqlIdentifier name, SqlDataTypeSpec type, SqlCharStringLiteral comment, SqlParserPos pos) {
		super(name, type, comment, pos);
	}

	@Override
	protected void unparseColumn(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.print(" ");
		ExtendedSqlType.unparseType(this.type, writer, leftPrec, rightPrec);
	}

}
