package org.example.flink._1_12.func;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

public class FirstColUdaf extends AggregateFunction<String, FirstColUdaf.Accum> {

    private transient DateTimeFormatter formatter;

    @Override
    public void open(FunctionContext context) throws Exception {
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getValue(Accum accumulator) {
        return accumulator.value;
    }

    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    public void accumulate(Accum accumulator, LocalDateTime col, String orderKey) {
        if (accumulator.orderKey == null || orderKey.compareTo(accumulator.orderKey) < 0) {
            accumulator.value = col.format(formatter);
            accumulator.orderKey = orderKey;
        }
    }

    public void accumulate(Accum accumulator, String col, String orderKey) {
        if (accumulator.orderKey == null || orderKey.compareTo(accumulator.orderKey) < 0) {
            accumulator.value = col;
            accumulator.orderKey = orderKey;
        }
    }

    public void accumulate(Accum accumulator, LocalDateTime col, LocalDateTime timestamp) {
        if (accumulator.timestamp == null || timestamp.isBefore(accumulator.timestamp)) {
            accumulator.value = col.format(formatter);
            accumulator.timestamp = timestamp;
        }
    }

    public void accumulate(Accum accumulator, String col, LocalDateTime timestamp) {
        if (accumulator.timestamp == null || timestamp.isBefore(accumulator.timestamp)) {
            accumulator.value = col;
            accumulator.timestamp = timestamp;
        }
    }

    public void merge(Accum accumulator, Iterable<Accum> it) {
        Iterator<Accum> iterator = it.iterator();
        while (iterator.hasNext()) {
            Accum b = iterator.next();
            if (b.orderKey == null) {
                if (accumulator.timestamp == null || b.timestamp.isBefore(accumulator.timestamp)) {
                    accumulator.value = b.value;
                    accumulator.timestamp = b.timestamp;
                }
            } else {
                if (accumulator.orderKey == null
                        || b.orderKey.compareTo(accumulator.orderKey) < 0) {
                    accumulator.value = b.value;
                    accumulator.orderKey = b.orderKey;
                }
            }
        }
    }

    public static class Accum {
        public String value;
        public String orderKey;
        public LocalDateTime timestamp;
    }
}
