package com.sonatel.yoonwi.launcher

import com.sonatel.yoonwi.classes.RandomTrafficStatesCreator
import com.sonatel.yoonwi.utils.CustomConfig
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
  *
  */
object Launcher {

  val logger = LogManager.getLogger(getClass)

  val hadoopConfig = new Configuration()

  def main(args : Array[String]): Unit ={

    /**
      * Checking if configuration file is provided
      */
      if(args.length == 0){
        logger.info("Le programme requiert un fichier de configuration pour s'exécuter")
        System.exit(0)
      }
    /**
      * Retrieving configuration file directory
      */
    val hdpConfigPath = args(0)

    /**
      * Loading configuration file
      */
    CustomConfig.load(hdpConfigPath+"/application.conf")

    /**/
    /*SPARK WAREHOUSE PATH*/
    val SPARK_WAREHOUSE_PATH=CustomConfig.SPARK_WAREHOUSE_PATH
    /*SPARK*/
    val MASTER = CustomConfig.MASTER
    val APPNAME = CustomConfig.APPNAME

    /*SECTION*/
    val EXPERIMENT_SECTION_ID = CustomConfig.EXPERIMENT_SECTION_ID
    val EXPERIMENT_SECTION_CRITICAL_VELOCITY = CustomConfig.EXPERIMENT_SECTION_CRITICAL_VELOCITY
    val EXPERIMENT_SECTION_FREE_VELOCITY = CustomConfig.EXPERIMENT_SECTION_FREE_VELOCITY
    val EXPERIMENT_SECTION_MAXIMAL_CONCENTRATION = CustomConfig.EXPERIMENT_SECTION_MAXIMAL_CONCENTRATION
    val EXPERIMENT_SECTION_CRITICAL_CONCENTRATION = CustomConfig.EXPERIMENT_SECTION_CRITICAL_CONCENTRATION

    /*MONGO*/
    val SLEEPING_INTERVAL = CustomConfig.SLEEPING_INTERVAL
    val  OUTPUT_URI = CustomConfig.OUTPUT_URI
    val COLLECTION = CustomConfig.COLLECTION

    //Creating a sparkSession : entry point of a spark application
    val sparkSession = SparkSession.builder()
      .master(MASTER)
      .appName(APPNAME)
      .config("spark.mongodb.output.uri", OUTPUT_URI)
      .getOrCreate()

    val trafficState = new RandomTrafficStatesCreator(sparkSession, COLLECTION, SLEEPING_INTERVAL, EXPERIMENT_SECTION_ID, EXPERIMENT_SECTION_CRITICAL_VELOCITY,
      EXPERIMENT_SECTION_FREE_VELOCITY, EXPERIMENT_SECTION_MAXIMAL_CONCENTRATION, EXPERIMENT_SECTION_CRITICAL_CONCENTRATION)

    trafficState.produceRecord(None) //produce continuously but with somme dealy heinh !
  }

}
