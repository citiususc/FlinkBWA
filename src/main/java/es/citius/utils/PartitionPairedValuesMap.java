package es.citius.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by claudia on 7/12/17.
 */
public class PartitionPairedValuesMap implements MapFunction<Tuple2<Long, Tuple2<String, String>>, Tuple2<String, String>> {

    public Tuple2<String, String> map(Tuple2<Long, Tuple2<String, String>> input) throws Exception {
        return input.f1;
    }
}
