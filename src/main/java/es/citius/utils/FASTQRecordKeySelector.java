package es.citius.utils;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
/**
 * Created by claudia on 7/12/17.
 */
public class FASTQRecordKeySelector implements KeySelector<Tuple2<Long, String>, Long> {
    public Long getKey(Tuple2<Long, String> record) throws Exception {
        return record.f0;
    }
}
