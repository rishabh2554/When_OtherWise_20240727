import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Medium_Questions {

  def main(args:Array[String]):Unit={

    val spark= SparkSession.builder()
      .appName("Medium_Questions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

   /* Question: How would you add a new column stock_level with values "Low" if quantity is less than 10,
    "Medium" if quantity is between 10 and 20, and "High" otherwise?*/

    val inventory = List(
      (1, 5),
      (2, 15),
      (3, 25)
    ).toDF("item_id", "quantity")

    val stock = inventory.select(col("item_id"),col("quantity"),when( col("quantity")<10,"Low" )
      .when(col("quantity")>=10 && col("quantity")<20,"Medium").otherwise("High").alias("stock_level"))

    stock.show()

    /*Question: How would you add a new column email_provider with values "Gmail" if email contains
      "gmail", "Yahoo" if email contains "yahoo", and "Other" otherwise?*/
    val customers = List(
      (1, "john@gmail.com"),
      (2, "jane@yahoo.com"),
      (3, "doe@hotmail.com")
    ).toDF("customer_id", "email")

    val domainDF = customers.select(col("customer_id"),col("email"),
      when(col("email").contains("gmail"),"Gmail")
        .when(col("email").contains("yahoo"),"Yahoo")
        .otherwise("Other").alias("email_provider"))

    domainDF.show()

/*    Question: How would you add a new column season with values "Summer" if order_date is in June,
    July, or August, "Winter" if in December, January, or February, and "Other" otherwise?*/

    val orders = List(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")

    val month = orders.select(col("order_id"),col("order_date"),
      from_unixtime(unix_timestamp(col("order_date"), "yyyy-MM-dd"),"MMMMM").alias("Month")
    ,when( from_unixtime(unix_timestamp(col("order_date"), "yyyy-MM-dd"),"MMMMM") === "July"
        || from_unixtime(unix_timestamp(col("order_date"), "yyyy-MM-dd"),"MMMMM") === "June"
        ||  from_unixtime(unix_timestamp(col("order_date"), "yyyy-MM-dd"),"MMMMM") === "Aug" , "Summer")
      .when( from_unixtime(unix_timestamp(col("order_date"), "yyyy-MM-dd"),"MMMMM") === "December"
        || from_unixtime(unix_timestamp(col("order_date"), "yyyy-MM-dd"),"MMMMM") === "January"
    ||  from_unixtime(unix_timestamp(col("order_date"), "yyyy-MM-dd"),"MMMMM") === "February" , "Winter")
        .otherwise("Other").alias("Season"))

    month.show()

    /*Question: How would you add a new column discount with values 0 if amount is less than 200, 10 if
    amount is between 200 and 1000, and 20 if amount is greater than 1000?*/


    val sales = List(
      (1, 100),
      (2, 1500),
      (3, 300)
    ).toDF("sale_id", "amount")

    val discountDF = sales.select( col("sale_id"),col("amount")
    ,when(col("amount")<200, 0)
    .when(col("amount")>=200 && col("amount")<1000 , 10)
    .when(col("amount")>=1000, 20).alias("discount"))

    discountDF.show()

    /*Question: How would you add a new column is_morning which is true if login_time is before 12:00,
    and false otherwise?*/

    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time")

    val morningDF = logins.withColumn(
         "is_morning",when($"login_time" <"12:00" , "True").otherwise("False")
    )

    morningDF.show()

  }

}
