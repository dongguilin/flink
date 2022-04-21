package org.apache.flink.table.sources.wmstrategies;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlWatermarkAssigner extends PunctuatedWatermarkAssigner {

	private long mills;

	public SqlWatermarkAssigner(String expr) {
		Matcher matcher = Pattern.compile("`(?<field>\\w+)`(?:\\s-\\sINTERVAL\\s)'(?<num>\\d+)'\\s(?<span>SECOND|MINUTE|HOUR)").matcher(expr);
		if (!matcher.matches()) {
			throw new IllegalArgumentException(expr);
		}

		long num = Long.parseLong(matcher.group("num"));
		String span = matcher.group("span");
		mills = mills(num, span);
	}

	private long mills(long num, String span) {
		if (span.equals("SECOND")) {
			return TimeUnit.SECONDS.toMillis(num);
		} else if (span.equals("MINUTE")) {
			return TimeUnit.MINUTES.toMillis(num);
		} else if (span.equals("HOUR")) {
			return TimeUnit.HOURS.toMillis(num);
		}
		throw new IllegalArgumentException("unsupport " + span);
	}

	@Override
	public Watermark getWatermark(Row row, long timestamp) {
		return new Watermark(timestamp - mills);
	}
}
