package es.citius.bwa;

/**
 * This class is used in case of need to use BWA from Java in a sequential way. Use: java -jar
 * BwaSeq.jar bwa mem ...
 *
 * @author José M. Abuín
 */
public class BwaSeq {

  public static void main(String[] args) {

    BwaJni.Bwa_Jni(args);
  }
}
