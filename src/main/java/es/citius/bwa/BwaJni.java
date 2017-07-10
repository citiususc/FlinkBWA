package es.citius.bwa;

import cz.adamh.utils.NativeUtils;

import java.io.IOException;

/**
 * Class that calls BWA functions by means of JNI
 *
 * @author José M. Abuín
 */
public class BwaJni {

	/**
	 * In order to generate es_citius_bwa_BvaJni.h file form the native folder
	 * you must comment the marked block of code and run the commands:
	 * cd <PROJECT_PATH>/src/main/java
	 * javac es/citius/bwa/BwaJni.java
	 * javah es.citius.bwa.BwaJni
	 */
	/* start comment block */
	static {
		try {
			NativeUtils.loadLibraryFromJar("/libbwa.so");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/* end comment block */

	/**
	 * Function to call BWA native main method from Java
	 * @param args A String array with the arguments to call BWA
	 * @return The BWA integer result value
	 */
	public static int Bwa_Jni(String[] args) {

		int[] lenStrings = new int[args.length];

		int i = 0;

		for (String arg : args) {
			lenStrings[i] = arg.length();
		}

		int returnCode = new BwaJni().bwa_jni(args.length, args, lenStrings);

		return returnCode;
	}

	//Declaration of native method
	private native int bwa_jni(int argc, String[] argv, int[] lenStrings);
}
