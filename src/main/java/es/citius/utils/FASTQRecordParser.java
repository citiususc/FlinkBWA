package es.citius.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Class that implements functionality of grouping FASTQ indexed PairRDD
 * @author Pål Karlsrud
 * @author Jose M. Abuin
 */
public class FASTQRecordParser implements FlatMapFunction<String, Tuple2<String, Long>> {
	public void flatMap(String textFile, Collector<Tuple2<String, Long>> out) throws Exception {
		String[] textLines = textFile.split("\r\n|\r|\n");
		AtomicLong idx = new AtomicLong(0);

		for (String line : textLines) {
			out.collect(new Tuple2<String, Long>(line,idx.getAndIncrement()));
		}

	}
}
