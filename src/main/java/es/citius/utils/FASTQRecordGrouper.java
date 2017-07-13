package es.citius.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FASTQRecordGrouper implements MapFunction<Tuple2<Long, String>,Tuple2<Long, Tuple2<Long, String>>> {

	public Tuple2<Long, Tuple2<Long, String>> map(Tuple2<Long, String> record) throws Exception {
		Long lineNo = record.f0;
		String info = record.f1;

		// We obtain the record number from the line number
		Long recordNo = (long) Math.floor(lineNo / 4);
		// We form the pair <String line, Long index inside record (0..3)>
		Tuple2<Long, String> newRecord = new Tuple2<Long, String>(lineNo % 4,info);

		// We return the data to group <Long record number,<String line, Long line index>>
		return new Tuple2<Long,Tuple2<Long, String>>(recordNo, newRecord);

	}
}
