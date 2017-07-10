# FlinkBWA
FlinkBWA is a new tool that exploits the capabilities of a Big Data technology as Apache Flink to boost the performance of one of the most widely adopted sequence aligner, the Burrows-Wheeler Aligner (BWA).

The current version of FlinkBWA (v0.1, July 2017) supports the following BWA algorithms:

* **BWA-MEM**
* **BWA-backtrack**
* **BWA-SW**

All of them work with single-reads and paired-end reads.

If you use **SparkBWA** or **FlinkBWA**, please cite this article:

José M. Abuin, Juan C. Pichel, Tomás F. Pena and Jorge Amigo. ["SparkBWA: Speeding Up the Alignment of High-Throughput DNA Sequencing Data"][5]. PLoS ONE 11(5), pp. 1-21, 2016.

A version for Hadoop is available [here](https://github.com/citiususc/BigBWA).

A version for Spark is available [here](https://github.com/citiususc/SparkBWA).


# Project structure
The project keeps a standard Maven structure. The source code is in the *src/main* folder. Inside it, we can find two subfolders:

* **java** - Here is where the Java code is stored.
* **native** - Here the BWA native code (C) and the glue logic for JNI is stored.

# Getting started

## Requirements
Requirements to build **FlinkBWA** are the same than the ones to [build BWA](https://github.com/lh3/bwa/blob/master/README.md), with the only exception that the *JAVA_HOME* environment variable should be defined. If not, you can define it in the */src/main/native/Makefile.common* file. 

It is also needed to include the flag *-fPIC* in the *Makefile* of the considered BWA version. To do this, the user just need to add this option to the end of the *CFLAGS* variable in the BWA Makefile. Considering bwa-0.7.15, the original Makefile contains:

	CFLAGS=		-g -Wall -Wno-unused-function -O2

and after the change it should be:

	CFLAGS=		-g -Wall -Wno-unused-function -O2 -fPIC

Additionaly, [Maven 3](https://maven.apache.org/install.html) is also required.

## Building
The default way to build **FlinkBWA** is:

	https://github.com/citiususc/FlinkBWA.git
	cd FlinkBWA
	mvn package

This will create the *target* folder, which will contain the *jar* file needed to run **FlinkBWA**:

* **citius-flink-bwa-1.0-SNAPSHOT.jar** - jar file to launch with Flink.

## Install Apache Flink

TODO

## Running FlinkBWA

TODO

## Accuracy

TODO
