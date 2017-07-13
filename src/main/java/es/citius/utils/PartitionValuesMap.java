package es.citius.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
/**
 * Created by claudia on 7/11/17.
 */
public class PartitionValuesMap implements MapFunction<Tuple2<Long, String>, String> {

    public String map(Tuple2<Long, String> input) throws Exception {
        return input.f1;
    }
}
