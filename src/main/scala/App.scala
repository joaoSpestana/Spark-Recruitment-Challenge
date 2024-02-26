import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object App{

  def main(args: Array[String]): Unit = {
    //Initiate a spark session to run locally
    val spark = SparkSession.builder().appName("Read Csv File").master("local").getOrCreate()
    //To bypass the Hadoop local FileSystem specify that I want to use the Bare Local File System
    spark.sparkContext.hadoopConfiguration.setClass("fs.file.impl",classOf[BareLocalFileSystem], classOf[FileSystem])
    //spark.sparkContext.setLogLevel("ERROR")
    //Specify that I want to use the legacy date format
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

    //Read the googleplaystore_user_reviews.csv
    val df_reviews = spark.read
      .format("csv")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .load("src/Files/googleplaystore_user_reviews.csv")

    //Read the googleplaystore.csv
    val df_apps = spark.read
      .format("csv")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .load("src/Files/googleplaystore.csv")

    //Run the fuctions, each corresponding to a different part
    val df_1 = part1(df_reviews)
    part2(df_apps, spark, "best_apps.csv")
    val df_3 = part3(df_apps)
    val path_df4 = part4(df_1, df_3, spark, "googleplaystore_cleaned")
    part5(df_3, spark, path_df4, "googleplaystore_metrics")

    spark.stop()
  }


  //Solution for the 1st part - This function receives as an argument the dataframe created from the googleplaystore_user_reviews.csv
  def part1(data:DataFrame): DataFrame={
    //Since in the challenge the default value for the "Average_Sentiment_Polarity" is set at 0, I replaced all missing values with 0
    val df =data.withColumn("Sentiment_Polarity", when(col("Sentiment_Polarity")==="nan" ,0).otherwise(col("Sentiment_Polarity")))
    df.groupBy("App").agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))
  }


  /*Solution for the 2nd part - This function receives as arguments:
        - The dataframe created from the googleplaystore.csv
        - The Spark session
        - The name I want to save the resulting dataframe as
  */
  def part2(data:DataFrame, spark: SparkSession, filename:String): Unit={
    //I interpreted as excluding the missing ratings, keeping the ratings that are realy above or equal to 4.0
    val df = data.filter(col("Rating")>=4.0 && col("Rating") =!= "NaN").orderBy(desc("Rating"))

    // As an option I decided to save the dataframe in desktop inside a "part2" folder
    val desktop_path = ("file:///" + System.getProperty("user.home") + "/Desktop/").replace("\\", "/") + "part2/"
    val file_path = desktop_path + filename

    //Due to the specified delimiter ("§") I used the following encoder to support the character
    df.coalesce(1).write.
      mode("overwrite").
      format("com.databricks.spark.csv").
      option("header", "true").
      option("delimiter", "§").
      option("encoding", "ISO-8859-1").
      csv(desktop_path)

    //Since I wasn't able to save the dataframe with the specific name, I opted to rename it afterwards
    //By first get the current name and then rename it
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path(desktop_path + "part*"))(0).getPath().getName()

    fs.rename(new Path(desktop_path + file), new Path(file_path))
    //Clean the folder by deleting _SUCCESS file since it is only a marker file
    fs.delete(new Path(desktop_path + "_SUCCESS"), true)

  }


  //Solution for the 3rd part - This function receives as a argument the dataframe created from the googleplaystore.csv
  def part3(data:DataFrame): DataFrame={
    // To do this part I decided to separate the solution into different steps

    //1st create a field with an Array of Categories
    val df_categories = data.groupBy("App").agg(collect_list("Category").as("Categories"))

    //2nd create a dataframe where for each app I only kept the row with the maximum number of reviews
    val w = Window.partitionBy("App")
    val df_maxReviews= data.withColumn("maxReviews", max("Reviews").over(w))
      .where(col("Reviews") === col("maxReviews"))
      .drop("maxReviews").withColumnRenamed("App", "App1")

    //3nd I joined the two resulting dataframes
    val df_join = df_categories.join(df_maxReviews, df_categories.col("App")===df_maxReviews.col("App1")).
      drop("App1", "Category")

    //4th I renamed the columns as specified in the challenge
    val df_renamed = df_join.withColumnRenamed("Current Ver","Current_Version")
      .withColumnRenamed("Content Rating","Content_Rating")
      .withColumnRenamed("Android Ver","Minimum_Android_Version")
      .withColumnRenamed("Last Updated","Last_Updated")

    //df_renamed.select(df_renamed("Size")).distinct.show(100)
    //df_renamed.select(df_renamed("Price")).distinct.show()

    /*5th I treated the Size column by converting it to MB:
          - The rows in M (MB) were cast to double
          - The rows in k were converted to MB (*0.001) and then cast to Double
          - The rows "Varies with device" I opted for replacing with -1 in order to distinguish them
     */
    val df_size=df_renamed.withColumn("Size", when(col("Size").contains("k") , (regexp_replace(col("Size"), "[^0-9]", "")*0.001).cast("Double")).
      when(col("Size")==="Varies with device", -1).otherwise(regexp_replace(col("Size"), "[^0-9]", "").cast("Double")))

    //The price was converted to €
    val df_price = df_size.withColumn("Price", (regexp_replace(col("Price"), "[^0-9]", "")*0.9).cast("Double"))

    val df_final = df_price.withColumn("Genres", split(col("Genres"), ";").cast("array<String>")).
      withColumn("Reviews", col("Reviews").cast("Long")).
      withColumn("Rating", col("Rating").cast("Double")).
      withColumn("Last_Updated", to_date(col("Last_Updated"), "MMMM dd, yyyy")).
      orderBy(desc("Reviews")).dropDuplicates("App")

    df_final
  }


  /*Solution for the 4th part - This function receives as arguments:
      - The dataframe created in part 1 (df_1)
      - The dataframe created in part 3 (df_3)
      - The Spark session
      - The name I want to save the resulting dataframe as
  */
  def part4(data1:DataFrame,data3:DataFrame, spark: SparkSession, filename:String): String={
    //Produce a Dataframe with all information by joining df_1 and df_3
    val df4 =  data3.join(data1.withColumnRenamed("App", "App1"), col("App")===col("App1")).
      drop("App1")

    //As an option I decided to save the dataframe in desktop inside a "part4" folder
    val desktop_path = ("file:///" + System.getProperty("user.home") + "/Desktop/").replace("\\", "/") + "part4/"
    val file_path = desktop_path + filename + ".gz.parquet"

    df4.coalesce(1).write.
      mode("overwrite").
      format("parquet").
      option("compression", "gzip").
      parquet(desktop_path)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path(desktop_path + "part*"))(0).getPath().getName()

    fs.rename(new Path(desktop_path + file), new Path(file_path))
    fs.delete(new Path(desktop_path + "_SUCCESS"), true)

    //This function returns the path that the file was saved (to be used in part 5)
    file_path
  }


  /*Solution for the 5th part - This function receives as arguments:
     - The dataframe created in part 3 (df_3)
     - The Spark session
     - The path that the file created in part4 was saved
     - The name I want to save the resulting dataframe as
 */
  def part5(data:DataFrame, spark: SparkSession, path:String, filename:String): Unit={
    // Since the challenge said to use the dataframe created in part3, to get the "Average_Sentiment_Polarity" I read the file created in part4
    val df4 = spark.read
      .format("parquet")
      .load(path)

    //And joined it to the df_3
    val df_join = data.join(df4.withColumnRenamed("App", "App1").select("App1", "Average_Sentiment_Polarity"), col("App")===col("App1")).
      drop("App1")

    //Used the explode function to get a row for each genre presented in the Array of the column "Genres"
    val df_explode = df_join.withColumn("Genre", explode(col("Genres")))

    val df_final = df_explode.groupBy("Genre").
      agg(count("App").as("Count"), avg("Rating").as("Average_Rating"), avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity"))

    //As an option I decided to save the dataframe in desktop inside a "part5" folder
    val desktop_path = ("file:///" + System.getProperty("user.home") + "/Desktop/").replace("\\", "/") + "part5/"
    val file_path = desktop_path + filename + ".gz.parquet"

    df_final.coalesce(1).write.
      mode("overwrite").
      format("parquet").
      option("compression", "gzip").
      parquet(desktop_path)

    df_final.show()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path(desktop_path + "part*"))(0).getPath().getName()

    fs.rename(new Path(desktop_path + file), new Path(file_path))
    fs.delete(new Path(desktop_path + "_SUCCESS"), true)
  }
}