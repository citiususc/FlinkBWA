package es.citius.utils;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Class that implements functionality of grouping FASTQ
 */
public class FASTQRecordCreator implements GroupReduceFunction<Tuple2<Long, Tuple2<String, Long>>, Tuple2<Long, String>> {

	public void reduce(Iterable<Tuple2<Long, Tuple2<String, Long>>> recordGroup, Collector<Tuple2<Long, String>> out) throws Exception {
		// We create the data to be contained inside the record
		String seqName 		= null;
		String seq			= null;
		String qual			= null;
		String extraSeqname	= null;
		Long idx = 0L;

		for (Tuple2<Long, Tuple2<String, Long>> item : recordGroup) {
			idx = item.f0;
			Tuple2<String, Long> record = item.getField(2);
			Long lineNo = record.f1;
			String line = record.f0;

			if (lineNo == 0) {
				seqName = line;
			}
			else if (lineNo == 1) {
				seq = line;
			}
			else if (lineNo == 2) {
				extraSeqname = line;
			}
			else {
				qual = line;
			}
		}

		// If everything went fine, we return the current record
		if (seqName != null && seq != null && qual != null && extraSeqname != null) {
			out.collect(new Tuple2<Long, String>(idx, String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual)));
		} else {
			System.err.println("Malformed record!");
			System.err.println(String.format("%d - %s\n%s\n%s\n%s\n", idx, seqName, seq, extraSeqname, qual));
		}
	}
}
