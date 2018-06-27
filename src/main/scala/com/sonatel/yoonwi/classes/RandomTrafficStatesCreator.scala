package com.sonatel.yoonwi.classes

import java.util.Calendar

import com.mongodb.client.MongoCollection
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.bson.Document

import scala.util.{Random => r}

/**
  *
  * @param sparkSession
  */
class RandomTrafficStatesCreator (sparkSession : SparkSession, collection : String, INTERVAL : Int, id : Int, vitesseCritique: Int, vitesseLibre : Int, capacite : Int, concentrationCritique : Int){

  /*val writeConfig : WriteConfig = WriteConfig(Map("uri"-> "mongodb://127.0.0.1:27017/yoonwi.etats"))
  var mongoConnector : MongoConnector=_*/

/*def randomly(): Unit ={
  mongoConnector = MongoConnector(writeConfig.asOptions)
  val now = Calendar.getInstance()
  var densite =0
  var etat =""
  val vitesse = r.nextInt(130) //vitesse moyenne aléatoire
  if(vitesse >= vitesseLibre){ //la vitesse moyenne est supérieure ou égale à la vitesse libre : cas d'une circulation fluide
    densite= r.nextInt(concentrationCritique/2) //la densité sera limitée à la moitié de la densité critique
  }
  if(vitesse < vitesseLibre && vitesse >= vitesseCritique){
    val start = concentrationCritique/2
    val end = concentrationCritique
    densite = start + r.nextInt((end - start +1)) //la densite dera limitée par la moitié et la concentration critique
  }
  if(vitesse < vitesseCritique){ //on a une congestion
    val start = concentrationCritique
    val end = capacite
    densite = start + r.nextInt((end - start +1))
  }
  val value = Seq(Row(id, vitesse, densite, now.get(Calendar.DAY_OF_WEEK), now.get(Calendar.HOUR_OF_DAY), now.get(Calendar.MINUTE)))

  //Writing value
  val d = r.nextFloat()*100 //configuring deadly
  if (d < 2){
    println("Som network dealy !")
    Thread.sleep((d*100).toLong)
  }

  val schema = List(
    StructField("id", IntegerType, false),
    StructField("vitesse", IntegerType, false),
    StructField("densite", IntegerType, false),
    StructField("day", StringType, false),
    StructField("hour", IntegerType, false),
    StructField("minute", IntegerType, false)
  ) //defining schema for traffic count event
  //Creating dataframe : use createDataFrale method for deployment environnment purposes (for local test, use toDF with spark.implicits._)
  val valueDF = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(value), StructType(schema))


  //Saving dataframe
  MongoSpark.save(valueDF.write.option("collection", "etats").mode("append")) //use MongoSpark to save dataframe
 /* new Document().append("id", 1).append("vitesse", 20).append("densite", 10).append("etat", "fluide").append("heure", 15).append("minute", 30)

  mongoConnector.withCollectionDo(writeConfig, {MongoCollection[Document] =>

    collection.insertOne(new Document().append("id", 1).append("vitesse", 20).append("densite", 10).append("etat", "fluide").append("heure", 15).append("minute", 30))
  })*/
}*/
  def produceRecord(numRecToProduce: Option[Int]): Unit = {
    def generateTrafficRecord(): DataFrame = {


      val now = Calendar.getInstance()
      var densite =0
      var etat =""
      val vitesse = r.nextInt(130) //vitesse moyenne aléatoire
      if(vitesse >= vitesseLibre){ //la vitesse moyenne est supérieure ou égale à la vitesse libre : cas d'une circulation fluide
        densite= r.nextInt(concentrationCritique/2) //la densité sera limitée à la moitié de la densité critique
        etat = "Fluide"
      }
      if(vitesse < vitesseLibre && vitesse >= vitesseCritique){
        val start = concentrationCritique/2
        val end = concentrationCritique
        densite = start + r.nextInt((end - start +1)) //la densite dera limitée par la moitié et la concentration critique
        etat = "Trafic"
      }
      if(vitesse < vitesseCritique){ //on a une congestion
        val start = concentrationCritique
        val end = capacite
        densite = start + r.nextInt((end - start +1))
        etat="Congestion"
      }

      val day = now.get(Calendar.DAY_OF_WEEK)
      val heure = now.get(Calendar.HOUR_OF_DAY)
      val minute = now.get(Calendar.MINUTE)

      val value = Seq(Row(id, vitesse, densite, etat, day, heure, minute))

      //print(s"Writing $value\n")
      val d = r.nextFloat() * 100
      if (d < 2) {
        //induction de décalage aléatoire : les véhicules ne se géolocalisent pas
        println("Some network dealy !")
        Thread.sleep((d*100000).toLong)
      }
      val schema = List(
        StructField("id", IntegerType, false),
        StructField("vitesse", IntegerType, false),
        StructField("densite", IntegerType, false),
        StructField("etat", StringType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType, false),
        StructField("minute", IntegerType, false)
      ) //defining schema for traffic count event
      //Creating dataframe : use createDataFrale method for deployment environnment purposes (for local test, use toDF with spark.implicits._)
     sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(value), StructType(schema)) //return a dataframe
    }
    numRecToProduce match {
      case Some(x) if x > 0 ⇒
       // producer.send(generateCarRecord(TOPIC_NAME))
        //Saving dataframe
        MongoSpark.save(generateTrafficRecord().write.option("collection", collection).mode("append")) //use MongoSpark to save dataframe
        Thread.sleep(INTERVAL)
        produceRecord(Some(x - 1))
      case None ⇒ //sending continuously
       // producer.send(generateCarRecord(TOPIC_NAME))
        MongoSpark.save(generateTrafficRecord().write.option("collection", "etats").mode("append")) //use MongoSpark to save dataframe
        Thread.sleep(INTERVAL)
        produceRecord(None)
      case _ ⇒
    }
  }

}
