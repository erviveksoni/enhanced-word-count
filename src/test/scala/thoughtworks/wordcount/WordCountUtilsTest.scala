package thoughtworks.wordcount

import WordCountUtils._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Dataset, SaveMode}
import thoughtworks.DefaultFeatureSpecWithSpark
import org.apache.spark.sql.functions._


class WordCountUtilsTest extends DefaultFeatureSpecWithSpark {
  feature("Split Words") {
    scenario("test splitting a dataset of words by spaces") {}

    scenario("test splitting a dataset of words by period") {
      import spark.implicits._
      val testData = Seq("Sample.text. Sample").toDS()

      When("Utility method runs")
      Then("Execute split function")
      val resultds = testData.splitWords(spark)
      val expectedColumns = 3
      resultds.count should be(expectedColumns)
    }

    scenario("test splitting a dataset of words by comma") {
      import spark.implicits._
      val testData = Seq("Sample text, Sample").toDS()
      When("Utility method runs")
      Then("Execute split function")
      val resultds = testData.splitWords(spark)
      val expectedColumns = 3
      resultds.count should be(expectedColumns)
    }

    scenario("test splitting a dataset of words by hypen") {
      import spark.implicits._
      val testData = Seq("Sample text - Sample").toDS()
      When("Utility method runs")
      Then("Execute split function")
      val resultds = testData.splitWords(spark)
      val expectedColumns = 3
      resultds.count should be(expectedColumns)
    }

    scenario("test splitting a dataset of words by semi-colon") {
      import spark.implicits._
      val testData = Seq("Sample text; Sample").toDS()
      When("Utility method runs")
      Then("Execute split function")
      val resultds = testData.splitWords(spark)
      val expectedColumns = 3
      resultds.count should be(expectedColumns)
    }
  }

  feature("Count Words") {
    scenario("basic test case") {
      import spark.implicits._
      val testData = Seq("Sample", "Sample", "Sample").toDS()
      When("Utility method runs")
      Then("Execute split function")
      val temp = testData.countByWord(spark)
      val expectedColumns = Array("word", "count")
      val expectedLines = List(("sample", 3)).toDF(expectedColumns: _*)

      temp.collect() should contain theSameElementsAs expectedLines.collect
    }

    scenario("should not aggregate dissimilar words") {
      import spark.implicits._
      val testData = Seq("Sample", "Sample", "Sample", "test").toDS()
      When("Utility method runs")
      Then("Execute split function")
      val temp = testData.countByWord(spark)
      val expectedColumns = Array("word", "count")
      val expectedLines = List(("sample", 3), ("test", 1)).toDF(expectedColumns: _*)

      temp.collect() should contain theSameElementsAs expectedLines.collect
    }

    scenario("test case insensitivity") {
      import spark.implicits._
      val testData = Seq("Sample", "Sample", "sample", "test").toDS()
      When("Utility method runs")
      Then("Execute split function")
      val temp = testData.countByWord(spark)
      val expectedColumns = Array("word", "count")
      val expectedLines = List(("sample", 3), ("test", 1)).toDF(expectedColumns: _*)

      temp.collect() should contain theSameElementsAs expectedLines.collect
    }
  }

  feature("Sort Words") {
    scenario("test ordering words") {
      import spark.implicits._
      val testData = Seq("Sample", "Sample", "sample", "test", "abc").toDS()
      When("Utility method runs")
      Then("Execute split function")
      val temp = testData.countByWord(spark)
      temp.show
      val expectedColumns = Array("word", "count")
      val expectedLines = List(("abc", 1), ("sample", 3), ("test", 1)).toDF(expectedColumns: _*)
      expectedLines.show

      temp.collect() should contain theSameElementsInOrderAs expectedLines.collect
    }
  }

}
