package thoughtworks.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

object WordCountUtils {

  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._
      val temp = dataSet
          .flatMap(line => line.split(' '))
          .flatMap(line => line.split(','))
          .flatMap(line => line.split('-'))
          .flatMap(line => line.split(';'))
          .flatMap(line => line.split('.'))
          .flatMap(line => line.split('\"'))
          .filter(record => !record.isEmpty)
          //.flatMap(line => line.split('"'))
      temp
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      import org.apache.spark.sql.functions._
      dataSet.select(lower($"value") as "word").groupBy("word").count.orderBy($"word")//.select(concat($"value", lit(","), $"count"))
    }
  }
}
