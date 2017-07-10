package es.citius.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FASTQRecordGrouper implements MapFunction<Tuple2<String, Long>,Tuple2<Long, Tuple2<String, Long>>> {

	public Tuple2<Long, Tuple2<String, Long>> map(Tuple2<String, Long> record) throws Exception {
		// We get string input and line number
		String info = record.getField(1);
		Long lineNo = record.getField(2);

		// We obtain the record number from the line number
		Long recordNo = (long) Math.floor(lineNo / 4);
		// We form the pair <String line, Long index inside record (0..3)>
		Tuple2<String, Long> newRecord = new Tuple2<String, Long>(info, lineNo % 4);

		// We return the data to group <Long record number,<String line, Long line index>>
		return new Tuple2<Long,Tuple2<String, Long>>(recordNo, newRecord);
	}
}
