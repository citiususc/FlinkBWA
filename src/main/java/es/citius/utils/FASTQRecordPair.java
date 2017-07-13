package es.citius.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by claudia on 7/12/17.
 */
public class FASTQRecordPair implements MapFunction<Tuple2<Tuple2<Long, String>, Tuple2<Long, String>>, Tuple2<Long, Tuple2<String, String>>> {
    public Tuple2<Long, Tuple2<String, String>> map(Tuple2<Tuple2<Long, String>, Tuple2<Long, String>> input) throws Exception {
        return Tuple2.of(input.f0.f0, Tuple2.of(input.f0.f1, input.f1.f1));
    }
}
