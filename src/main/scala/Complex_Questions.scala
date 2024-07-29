import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types


object Complex_Questions {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("Complex_Questions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

/*    Question: How would you add a new column category with values "Young & Low Salary" if age is less
    than 30 and salary is less than 35000, "Middle Aged & Medium Salary" if age is between 30 and 40
    and salary is between 35000 and 45000, and "Old & High Salary" otherwise?*/

    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")


    val categoryDF = employees.select( col("employee_id"),col("age"),col("salary")
      ,when(col("age")<30 && col("salary") <35000,"Young & Low Salary")
      .when((col("age")>=30 && col("age")<40) && (col("salary") >= 35000 && col("salary")<45000),"Middle Aged & Medium Salary")
      .otherwise("Old & High Salary").alias("category")

    )

    categoryDF.show()

    /*Question: How would you add two new columns, feedback with values "Bad" if rating is less than 3,
    "Good" if rating is 3 or 4, and "Excellent" if rating is 5, and is_positive with values true if rating is
      greater than or equal to 3, and false otherwise?*/

    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")


    val columnsDF = reviews
      .withColumn( "feedback",when(col("rating")<3 , "Bad").when( (col("rating")===3) || (col("rating")===4),"Good").when(col("rating")===5,"Excellent"))
      .withColumn("is_positive",when(col("rating")>=3,"Ture").otherwise("False"))

    columnsDF.show()

   /* Question: How would you add a new column content_category with values "Animal Related" if
    content contains "fox", "Placeholder Text" if content contains "Lorem", and "Tech Related" if content
    contains "Spark"?*/

    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")

    val content_categoryDF = documents.select(col("doc_id"),col("content")
     ,when(col("content").contains("fox"), "Animal Related").when(col("content").contains("Lorem"),"Placeholder Text")
    .when(col("content").contains("Spark"),"Tech Related").alias("content_category") )

    content_categoryDF.show()

    /*Question: How would you add a new column task_duration which is "Short" if the difference
      between end_date and start_date is less than 7 days, "Medium" if it is between 7 and 14 days, and
    "Long" otherwise?*/

    val tasks = List(
    (1, "2024-07-01", "2024-07-10"),
    (2, "2024-08-01", "2024-08-15"),
    (3, "2024-09-01", "2024-09-05")
    ).toDF("task_id", "start_date", "end_date")

    val taskDaysDF = tasks.select(col("task_id"),col("start_date"),col("end_date"),
      (from_unixtime(unix_timestamp(col("end_date"),"yyyy-MM-dd"),"dd")
        - from_unixtime(unix_timestamp(col("start_date"),"yyyy-MM-dd"),"dd")).alias("DaysDiff"),
      when((from_unixtime(unix_timestamp(col("end_date"),"yyyy-MM-dd"),"dd")
        - from_unixtime(unix_timestamp(col("start_date"),"yyyy-MM-dd"),"dd") ) <7, "Short")
        .when((from_unixtime(unix_timestamp(col("end_date"),"yyyy-MM-dd"),"dd")
          - from_unixtime(unix_timestamp(col("start_date"),"yyyy-MM-dd"),"dd") ) >=7 &&
          (from_unixtime(unix_timestamp(col("end_date"),"yyyy-MM-dd"),"dd")
            - from_unixtime(unix_timestamp(col("start_date"),"yyyy-MM-dd"),"dd") )<14, "Medium")
        .otherwise("Long").alias("task_duration")    )

    taskDaysDF.show()



  }




}
