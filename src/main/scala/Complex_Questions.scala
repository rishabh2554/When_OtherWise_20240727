import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types


object Complex_Questions {

  def main(args:Array[String]):Unit= {

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


    val categoryDF = employees.select(col("employee_id"), col("age"), col("salary")
      , when(col("age") < 30 && col("salary") < 35000, "Young & Low Salary")
        .when((col("age") >= 30 && col("age") < 40) && (col("salary") >= 35000 && col("salary") < 45000), "Middle Aged & Medium Salary")
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
      .withColumn("feedback", when(col("rating") < 3, "Bad").when((col("rating") === 3) || (col("rating") === 4), "Good").when(col("rating") === 5, "Excellent"))
      .withColumn("is_positive", when(col("rating") >= 3, "Ture").otherwise("False"))

    columnsDF.show()

    /* Question: How would you add a new column content_category with values "Animal Related" if
    content contains "fox", "Placeholder Text" if content contains "Lorem", and "Tech Related" if content
    contains "Spark"?*/

    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")

    val content_categoryDF = documents.select(col("doc_id"), col("content")
      , when(col("content").contains("fox"), "Animal Related").when(col("content").contains("Lorem"), "Placeholder Text")
        .when(col("content").contains("Spark"), "Tech Related").alias("content_category"))

    content_categoryDF.show()

    /*Question: How would you add a new column task_duration which is "Short" if the difference
      between end_date and start_date is less than 7 days, "Medium" if it is between 7 and 14 days, and
    "Long" otherwise?*/

    val tasks = List(
      (1, "2024-07-01", "2024-07-10"),
      (2, "2024-08-01", "2024-08-15"),
      (3, "2024-09-01", "2024-09-05")
    ).toDF("task_id", "start_date", "end_date")

    val taskDaysDF = tasks.select(col("task_id"), col("start_date"), col("end_date"),
      (from_unixtime(unix_timestamp(col("end_date"), "yyyy-MM-dd"), "dd")
        - from_unixtime(unix_timestamp(col("start_date"), "yyyy-MM-dd"), "dd")).alias("DaysDiff"),
      when((from_unixtime(unix_timestamp(col("end_date"), "yyyy-MM-dd"), "dd")
        - from_unixtime(unix_timestamp(col("start_date"), "yyyy-MM-dd"), "dd")) < 7, "Short")
        .when((from_unixtime(unix_timestamp(col("end_date"), "yyyy-MM-dd"), "dd")
          - from_unixtime(unix_timestamp(col("start_date"), "yyyy-MM-dd"), "dd")) >= 7 &&
          (from_unixtime(unix_timestamp(col("end_date"), "yyyy-MM-dd"), "dd")
            - from_unixtime(unix_timestamp(col("start_date"), "yyyy-MM-dd"), "dd")) < 14, "Medium")
        .otherwise("Long").alias("task_duration"))

    taskDaysDF.show()


    /*Question: How would you add a new column order_type with values "Small & Cheap" if quantity is
      less than 10 and total_price is less than 200, "Bulk & Discounted" if quantity is greater than or equal
      to 10 and total_price is less than 200, and "Premium Order" otherwise?*/

    val orders = List(
      (1, 5, 100),
      (2, 10, 150),
      (3, 20, 300)
    ).toDF("order_id", "quantity", "total_price")

    val typeDF = orders.withColumn("order_type", when((col("quantity") < 10 && col("total_price") < 200), "Small & Cheap")
      .when((col("quantity") >= 10 && col("total_price") < 200), "Bulk & Discounted").otherwise("Premium Order"))
    typeDF.show()

    /*Question: How would you add two new columns, is_hot with values true if temperature is greater
    than 30, and false otherwise, and is_humid*/

    val weather = Seq(
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)
    ).toDF("day_id", "temperature", "humidity")

    val conditionDF = weather.withColumn("is_hot", when(col("temperature") > 30, "True").otherwise("False"))
      .withColumn("is_humid", when(col("humidity") > 60, "True").otherwise("False"))

    conditionDF.show()


    /*  Question: How would you add two new columns, math_grade with values "A" if math_score is
      greater than 80, "B" if it is between 60 and 80, and "C" otherwise, and english_grade with values "A"
    if english_score is greater than 80, "B" if it is between 60 and 80, and "C" otherwise?*/
    val scores = List(
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ).toDF("student_id", "math_score", "english_score")


    val gradeDF = scores.withColumn("math_grade", when(col("math_score") > 80, "A")
                             .when((col("math_score") >= 60 && col("math_score") <= 80), "B").otherwise("C"))
                        .withColumn("english_grade", when(col("english_score") > 80, "A")
                             .when(col("english_score") >= 60 && col("english_score") <= 80, "B").otherwise("C"))

       gradeDF.show

    /*Question: How would you add a new column email_domain with values "Gmail" if email_address
    contains "gmail", "Yahoo" if it contains "yahoo", and "Hotmail" otherwise?*/

    val emails = List(
      (1, "user@gmail.com"),
      (2, "admin@yahoo.com"),
      (3, "info@hotmail.com")
    ).toDF("email_id", "email_address")

    val domainDF = emails.withColumn("email_domain", when(col("email_address").contains("gmail"),"Gmail")
    .when(col("email_address").contains("yahoo"),"Yahoo").otherwise("Hotmail"))

    domainDF.show

   /* Question: How would you add a new column quarter with values "Q1" if payment_date is in January,
    February, or March, "Q2" if in April, May, or June, "Q3" if in July, August, or September, and "Q4"
    otherwise?*/
    val payments = List(
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ).toDF("payment_id", "payment_date")

    val quarterFD = payments.withColumn("Month",from_unixtime( unix_timestamp(col("payment_date"), "yyyy-MM-dd"),"MMMMM" ))
      .withColumn( "quarter",
        when(from_unixtime( unix_timestamp(col("payment_date"), "yyyy-MM-dd"),"MMMMM" )
          .isin("January","February","March"),"Q1")
        .when(from_unixtime( unix_timestamp(col("payment_date"), "yyyy-MM-dd"),"MMMMM" )
          .isin("April","May","June"),"Q2")
        .when(from_unixtime( unix_timestamp(col("payment_date"), "yyyy-MM-dd"),"MMMMM" )
          .isin("July","August","September"),"Q3").otherwise("Q4")
      )
    quarterFD.show








  }




}
