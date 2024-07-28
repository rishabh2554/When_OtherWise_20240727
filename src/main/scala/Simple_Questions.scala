

import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession


object Simple_Questions {

  def main(args:Array[String]):Unit ={
 val spark=SparkSession.builder()
   .appName("IS_ADULT")
   .master("local[*]")
   .getOrCreate()

import spark.implicits._

    /*Question: How would you add a new column is_adult which is true if the age is greater than or equal
to 18, and false otherwise?*/
    val employees = List(
      (1, "John", 28),
      (2, "Jane", 35),
      (3, "Doe", 22),
      (4, "Tom", 12) )
      .toDF("id", "name", "age")

    val adultDF = employees.select(col("id"),col("name"),col("age"),
                  when(col("age")>= 18, "True").otherwise("False").alias("is_adult"))

    adultDF.show()


   /* Question: How would you add a new column grade with values "Pass" if score is greater than or
    equal to 50, and "Fail" otherwise?*/
    val grades = List(
      (1, 85),
      (2, 42),
      (3, 73)
    ).toDF("student_id", "score")

    val gradeDF = grades.select(col("student_id"),col("score"),
      when(col("score")>= 50, "Pass").otherwise("Fail").alias("grades"))

    gradeDF.show()

    /*Question: How would you add a new column category with values "High" if amount is greater than
      1000, "Medium" if amount is between 500 and 1000, and "Low" otherwise?*/
    val transactions = List(
      (1, 1000),
      (2, 200),
      (3, 5000),
      (4, 600)
    ).toDF("transaction_id", "amount")

   val categoryDF = transactions.select(col("transaction_id"),col("amount")
   , when(col("amount")>=1000,"High")
       .when(col("amount")<1000 && col("amount")>=500,"Medium")
       .otherwise("Low").alias("category"))

    categoryDF.show()

  /*  Question: How would you add a new column price_range with values "Cheap" if price is less than 50,
    "Moderate" if price is between 50 and 100, and "Expensive" otherwise?*/

    val products = List(
      (1, 30.5),
      (2, 150.75),
      (3, 75.25)
    ).toDF("product_id", "price")

val rangeDF = products.withColumn("price_range", when(col("price")<50,"Cheap").when(col("price")>=50 && col("price")<100,"Moderate").otherwise("Expensive"))
    rangeDF.show()

/*    Question: How would you add a new column is_holiday which is true if the date is "2024-12-25" or
      "2025-01-01", and false otherwise?*/

    val events = List(
      (1, "2024-07-27"),
      (2, "2024-12-25"),
      (3, "2025-01-01")
    ).toDF("event_id", "date")

    val holidayDF = events.withColumn("is_holiday",when(col("date")=== "2024-12-25" || col("date")=== "2025-01-01" ,"True")
      .otherwise("False") )
    holidayDF.show()


  }

}
