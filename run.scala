
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source
import smote.smote
import utils.KeelParser
import tomekLinks.smoteTomekLink


object run {
  def main(args: Array[String]) {
     
     //Desactivate Log
     Logger.getLogger("org").setLevel(Level.OFF)
     var logger = Logger.getLogger(this.getClass())
     
     if (args.length != 1) {
		   logger.error("=> wrong parameters number")
			 System.err.println("Configuration\n\t")
		   System.exit(1)
		 }
     
     // Reading parameters
     val configurationParam = args(0)
	   val args_1 = Source.fromFile(configurationParam).getLines.toArray
     println(args_1.deep.toString())
     val options = args_1.map { arg => arg.dropWhile(_ == '-').split(" = ") match {
				                        case Array(opt, v) => (opt -> v)
				                        case Array(opt) => (opt -> "") 
				                        case _ => throw new IllegalArgumentException("Invalid argument: "+args)
				                       }}.toMap
				                       
	  //Remember inputPath + in headerFile and inputFile
	  //Read in general inputs, set default in case of problems
		val seed = options.getOrElse("seed", "12345678").toInt
		val inputPath = options.getOrElse("inputPath", "ECBDL14_mbd/")
		val headerFile = inputPath + options.getOrElse("headerFile","ecbdl14.header")
    val inputFile = inputPath + options.getOrElse("inputFile","ecbdl14tra.data")
		val outputDirectory = options.getOrElse("outputDirectory","ECBDL14SMOTE")
		val outputFile = options.getOrElse("outputFile","SMOTE")
		val minorityClass = options.getOrElse("minorityClass","positive")
		val oversamplingPctg = options.getOrElse("oveR","0.1").toDouble
    val k = options.getOrElse("K","5").toInt
		val delimiter = options.getOrElse("delimiter",",")
		val numPartitions = options.getOrElse("nMaps","128").toInt
		val numReducers = options.getOrElse("nReducers","64").toInt
		val numIterations = options.getOrElse("numIterations", "1").toInt
		
				                       
     
     //Basic Spark setup
	   val jobName = "execute SMOTE" + "-" +"("+numPartitions+")" 
	   val conf = new SparkConf().setAppName(jobName)
	   //val conf = new SparkConf().setAppName(jobName).setMaster("local[*]")
	   val sc = new SparkContext(conf)
				                       
     
		 //Here some code to read the header file which will be used to transform the data into LabeledPoints (Spark API).
     val typeConversion = KeelParser.getParserFromHeader(sc,headerFile)
	   val bcTypeConv = sc.broadcast(typeConversion)

     
	   //Execute SMOTE or Tomek-Links
	   smote.runSMOTE(sc, inputFile, delimiter, minorityClass, k, oversamplingPctg, numPartitions, numReducers, typeConversion,numIterations, outputDirectory,outputFile)
	   //smoteTomekLink.runSMOTETomekLink(sc, inputFile, numPartitions, numReducers, typeConversion, numIterations, outputFile)
	   println("The algorithm has finished running")
		 sc.stop()
	   
    }
}