/**
 * Created by claudia on 6/28/17.
 */
package es.citius.main;

import es.citius.flink.FlinkBwaInterpreter;

public class FlinkBWA {

    /**
     *
     * @param args Argments from command line
     */
    public static void main(String[] args) {

        // create bwa interpreter instance using args options
        FlinkBwaInterpreter bwa = new FlinkBwaInterpreter(args);

        //run bwa interpreter
        bwa.runBwa();
    }
}
