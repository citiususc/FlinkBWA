package es.citius.bwa;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.Iterator;

/**
 * Class to perform the alignment over a split from the DataSet of single reads
 *
 * @author claudiai
 */
public class BwaSingleAlignment extends BwaAlignmentBase implements MapPartitionFunction<String, Iterator<String>> {

	/**
	 * Constructor
	 * @param env The Flink execution environment
	 * @param bwaInterpreter The BWA interpreter object to use
	 */
	public BwaSingleAlignment(ExecutionEnvironment env, Bwa bwaInterpreter) {
		super(env, bwaInterpreter);
	}

	public void mapPartition(Iterable<String> iterable, Collector<Iterator<String>> out) throws Exception {
		LOG.info("["+this.getClass().getName()+"] :: Tmp dir: " + this.tmpDir);

		String fastqFileName1;

		long timestamp = System.currentTimeMillis();

		if(this.tmpDir.lastIndexOf("/") == this.tmpDir.length()-1) {
			fastqFileName1 = this.tmpDir + this.appId + "-dataset" + timestamp + "_1";

		}
		else {
			fastqFileName1 = this.tmpDir + "/" + this.appId + "-dataset" + timestamp + "_1";

		}

		LOG.info("["+this.getClass().getName()+"] :: Writing file: " + fastqFileName1);

		File FastqFile1 = new File(fastqFileName1);
		FileOutputStream fos1;
		BufferedWriter bw1;

		try {
			fos1 = new FileOutputStream(FastqFile1);
			bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

			Iterator<String> reads = iterable.iterator();

			while (reads.hasNext()) {
				String record = reads.next();

				bw1.write(record);
				bw1.newLine();
			}

			bw1.flush();
			bw1.close();

			// This is where the actual local alignment takes place
			out.collect(this.runAlignmentProcess(timestamp, fastqFileName1, null));

			// Delete the temporary file, as is have now been copied to the output directory
			FastqFile1.delete();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			LOG.error("["+this.getClass().getName()+"] "+e.toString());
		}
	}
}
