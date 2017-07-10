package es.citius.flink;

import es.citius.bwa.Bwa;
import es.citius.bwa.BwaOptions;
import es.citius.utils.FASTQRecordCreator;
import es.citius.utils.FASTQRecordGrouper;
import es.citius.utils.FASTQRecordParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/**
 * Created by claudia on 6/28/17.
 */
public class FlinkBwaInterpreter {

    private static final Log    LOG = LogFactory.getLog(FlinkBwaInterpreter.class);
    private  ParameterTool    params = ParameterTool.fromSystemProperties();
    private final ExecutionEnvironment  env = ExecutionEnvironment.getExecutionEnvironment(); // Flink execution environment
    private BwaOptions          options; // Options for BWA

    /**
     * Constructor used in the main method
     *
     * @param args - arguments received from console when lounching FlinkBWA with Flink
     */
    public FlinkBwaInterpreter(String[] args) {
        this.options = new BwaOptions(args);
        this.params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(this.params);
    }

    /**
     * Constructor to build the BwaInterpreter object from the Flink shell
     * When creating a BwaInterpreter object from the Flink shell,
     * the BwaOptions and the Flink Execution Environment objects need
     * to be passed as argument.
     *
     * @param options The BwaOptions object initialized with the user options
     * @return The BwaInterpreter object with its options initialized.
     */
    public FlinkBwaInterpreter(BwaOptions options) {
        this.options = options;
        this.params = ParameterTool.fromSystemProperties();
        env.getConfig().setGlobalJobParameters(this.params);
    }

    /**
     * Function to load a FASTQ file into DataSet<Tuple2<Long, String>>
     * @param env The ExecutionEnvironment to use
     * @param path The path to the FASTQ file
     * @return A DataSet containing <Tuple2<Long Read ID, String Read>>
     */
    public static DataSet<Tuple2<Long, String>> loadFastq(ExecutionEnvironment env, String path) {
        DataSet<String> fastqLines = env.readTextFile(path);

        // Determine which FASTQ record the line belongs to.
        DataSet<Tuple2<Long, Tuple2<String, Long>>> fastqLinesByRecordNum = fastqLines.flatMap(new FASTQRecordParser()).map(new FASTQRecordGrouper());

        // Group group the lines which belongs to the same record, and concatenate them into a record.

        return fastqLinesByRecordNum.groupBy(0).reduceGroup(new FASTQRecordCreator());
    }

    /**
     * Method to perform and handle the single reads sorting
     * @return A DataSet containing the strings with the sorted reads from the FASTQ file
     */
    private DataSet<String> handleSingleReadsSorting() {
        DataSet<String> reads = null;

        long startTime = System.nanoTime();

        LOG.info("["+this.getClass().getName()+"] :: Not sorting in HDFS. Timing: " + startTime);

        // Read the FASTQ file from HDFS using the FastqInputFormat class
        DataSet<Tuple2<Long, String>> singleReadsKeyVal = loadFastq(this.env, this.options.getInputPath());

//        // Sort in memory with no partitioning
//        if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
//            // First, the join operation is performed. After that,
//            // a sortByKey. The resulting values are obtained
//            readsRDD = singleReadsKeyVal.sortByKey().values();
//            LOG.info("["+this.getClass().getName()+"] :: Sorting in memory without partitioning");
//        }
//
//        // Sort in memory with partitioning
//        else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
//            singleReadsKeyVal = singleReadsKeyVal.repartition(options.getPartitionNumber());
//            readsRDD = singleReadsKeyVal.sortByKey().values();//.persist(StorageLevel.MEMORY_ONLY());
//            LOG.info("["+this.getClass().getName()+"] :: Repartition with sort");
//        }
//
//        // No Sort with no partitioning
//        else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
//            LOG.info("["+this.getClass().getName()+"] :: No sort and no partitioning");
//            readsRDD = singleReadsKeyVal.values();
//        }
//
//        // No Sort with partitioning
//        else {
//            LOG.info("["+this.getClass().getName()+"] :: No sort with partitioning");
//            int numPartitions = singleReadsKeyVal.partitions().size();
//
//			/*
//			 * As in previous cases, the coalesce operation is not suitable
//			 * if we want to achieve the maximum speedup, so, repartition
//			 * is used.
//			 */
//            if ((numPartitions) <= options.getPartitionNumber()) {
//                LOG.info("["+this.getClass().getName()+"] :: Repartition with no sort");
//            }
//            else {
//                LOG.info("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort");
//            }
//
//            readsRDD = singleReadsKeyVal
//                    .repartition(options.getPartitionNumber())
//                    .values();
//            //.persist(StorageLevel.MEMORY_ONLY());
//
//        }

        long endTime = System.nanoTime();
        LOG.info("["+this.getClass().getName()+"] :: End of sorting. Timing: " + endTime);
        LOG.info("["+this.getClass().getName()+"] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");

        return reads;
    }

    /**
     * Method to perform and handle the paired reads sorting
     * @return A DataSet containing grouped reads from the paired FASTQ files
     */
    private DataSet<Tuple2<String, String>> handlePairedReadsSorting() {
        DataSet<Tuple2<String, String>> reads = null;

        long startTime = System.nanoTime();

        LOG.info("["+this.getClass().getName()+"] ::Not sorting in HDFS. Timing: " + startTime);

        // Read the two FASTQ files from HDFS using the loadFastq method. After that, a Flink join operation is performed
//        DataSet<Tuple2<Long, String>> datasetTmp1 = loadFastq(this.env, options.getInputPath());
//        DataSet<Tuple2<Long, String>> datasetTmp2 = loadFastq(this.env, options.getInputPath2());
//        DataSet<Long, Tuple2<String, String>> pairedReadsRDD = datasetTmp1.join(datasetTmp2);

////        datasetTmp1.unpersist();
////        datasetTmp2.unpersist();
////
////        // Sort in memory with no partitioning
////        if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
////            reads = pairedReadsRDD.sortByKey().values();
////            LOG.info("["+this.getClass().getName()+"] :: Sorting in memory without partitioning");
////        }
//
//        // Sort in memory with partitioning
//        else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
//            pairedReadsRDD = pairedReadsRDD.repartition(options.getPartitionNumber());
//            reads = pairedReadsRDD.sortByKey().values();//.persist(StorageLevel.MEMORY_ONLY());
//            LOG.info("["+this.getClass().getName()+"] :: Repartition with sort");
//        }
//
//        // No Sort with no partitioning
//        else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
//            LOG.info("["+this.getClass().getName()+"] :: No sort and no partitioning");
//        }
//
//        // No Sort with partitioning
//        else {
//            LOG.info("["+this.getClass().getName()+"] :: No sort with partitioning");
//            int numPartitions = pairedReadsRDD.partitions().size();
//
//			/*
//			 * As in previous cases, the coalesce operation is not suitable
//			 * if we want to achieve the maximum speedup, so, repartition
//			 * is used.
//			 */
//            if ((numPartitions) <= options.getPartitionNumber()) {
//                LOG.info("["+this.getClass().getName()+"] :: Repartition with no sort");
//            }
//            else {
//                LOG.info("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort");
//            }
//
//            reads = pairedReadsRDD
//                    .repartition(options.getPartitionNumber())
//                    .values();
//            //.persist(StorageLevel.MEMORY_ONLY());
//        }

        long endTime = System.nanoTime();

        LOG.info("["+this.getClass().getName()+"] :: End of sorting. Timing: " + endTime);
        LOG.info("["+this.getClass().getName()+"] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");

        return reads;
    }

    /**
     *
     * @param bwa The Bwa object to use
     * @param reads The RDD containing the paired reads
     * @return A list of strings containing the resulting sam files where the output alignments are stored
     */
//    private List<String> MapSingleBwa(Bwa bwa, DataSet<String> reads) {
//        // The mapPartition is used over this DataSet to perform the alignment. The resulting sam filenames are returned
//        return reads.mapPartition(new BwaSingleAlignment(reads.getExecutionEnvironment(), bwa)).collect();
//    }

    /**
     * Runs BWA with the specified options
     *
     * @brief This function runs BWA with the input data selected and with the options also selected
     *     by the user.
     */
    public void runBwa() {
        LOG.info("["+this.getClass().getName()+"] :: Starting BWA");
        Bwa bwa = new Bwa(this.options);

        List<String> returnedValues;
//        if (bwa.isPairedReads()) {
//            DataSet<Tuple2<String, String>> reads = handlePairedReadsSorting();
////            returnedValues = MapPairedBwa(bwa, reads);
//        } else {
            DataSet<String> reads = handleSingleReadsSorting();
//            returnedValues = MapSingleBwa(bwa, reads);
//        }
//
//        // In the case of use a reducer the final output has to be stored in just one file
//        if(this.options.getUseReducer()) {
//            try {
//                FileSystem fs = FileSystem.get(this.conf);
//
//                Path finalHdfsOutputFile = new Path(this.options.getOutputHdfsDir() + "/FullOutput.sam");
//                FSDataOutputStream outputFinalStream = fs.create(finalHdfsOutputFile, true);
//
//                // We iterate over the resulting files in HDFS and agregate them into only one file.
//                for (int i = 0; i < returnedValues.size(); i++) {
//                    LOG.info("JMAbuin:: SparkBWA :: Returned file ::" + returnedValues.get(i));
//                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(returnedValues.get(i)))));
//
//                    String line;
//                    line = br.readLine();
//
//                    while (line != null) {
//                        if (i == 0 || !line.startsWith("@")) {
//                            //outputFinalStream.writeBytes(line+"\n");
//                            outputFinalStream.write((line + "\n").getBytes());
//                        }
//
//                        line = br.readLine();
//                    }
//                    br.close();
//
//                    fs.delete(new Path(returnedValues.get(i)), true);
//                }
//
//                outputFinalStream.close();
//                fs.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//                LOG.error(e.toString());
//            }
//        }
	}
}
