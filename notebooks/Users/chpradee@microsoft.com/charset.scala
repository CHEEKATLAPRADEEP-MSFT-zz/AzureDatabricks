// Databricks notebook source
val df = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("sep", ";")
  .option("quote", "\"")
  .load("/FileStore/tables/azurewala.csv")
display(df)

// COMMAND ----------

val df = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
 .option("multiLine", "true")
  .option("sep", ";")
  .option("quote", "\"")
   .option("encoding", "SJIS")
  .load("/FileStore/tables/azurewala.csv")
display(df)

// COMMAND ----------

val df = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("encoding", "SJIS")
  .load("/FileStore/tables/azurewala.csv")
display(df)

// COMMAND ----------

val df = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("encoding", "SJIS")
  .load("/FileStore/tables/export__3_-6461d.csv")
display(df)

// COMMAND ----------

val jsonDF = sqlContext.read.format("csv")
      .option("multiline", "true")
      .option("encoding", "SJIS")
      .load("/FileStore/tables/azurewala.csv")
display(jsonDF)

// COMMAND ----------

val fileName = "/FileStore/tables/azurewala.csv"
    val schema = new StructType().add("id", StringType).add("company", StringType).add("date", StringType).add("price", StringType).add("signature", StringType)
    val jsonDF = spark.read.schema(schema)
      .option("multiline", "true")
      .option("encoding", "UTF-16")
      .json(testFile(fileName))

// COMMAND ----------

   val cars = spark
      .read
      .format("csv")
      .option("header", "true")
  
      .option("sep", "þ")
      .load("/FileStore/tables/azurewala.csv")
  val cars
display(cars)

// COMMAND ----------

test("encoding in multiLine mode") {
    val df = spark.range(3).toDF()
    Seq("UTF-8", "ISO-8859-1", "CP1251", "US-ASCII", "UTF-16BE", "UTF-32LE").foreach { encoding =>
      Seq(true, false).foreach { header =>
        withTempPath { path =>
          df.write
            .option("encoding", encoding)
            .option("header", header)
            .csv(path.getCanonicalPath)
          val readback = spark.read
            .option("multiLine", true)
            .option("encoding", encoding)
            .option("inferSchema", true)
            .option("header", header)
            .csv(path.getCanonicalPath)
          checkAnswer(readback, df)

// COMMAND ----------

test("using spark.sql.columnNameOfCorruptRecord") {
    withSQLConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.key -> "_unparsed") {
      val csv = "dbfs:/FileStore/tables/azurewala.csv""
      val df = spark.read
        .schema("a int, _unparsed string")
        .csv(Seq(csv).toDS())

      checkAnswer(df, Row(null, csv))
    }
  }

  test("encoding in multiLine mode") {
    val df = spark.range(3).toDF()
    Seq("UTF-8", "ISO-8859-1", "CP1251", "US-ASCII", "UTF-16BE", "UTF-32LE").foreach { encoding =>
      Seq(true, false).foreach { header =>
        withTempPath { path =>
          df.write
            .option("encoding", encoding)
            .option("header", header)
            .csv(path.getCanonicalPath)
          val readback = spark.read
            .option("multiLine", true)
            .option("encoding", ISO-8859-1)
            .option("inferSchema", true)
            .option("header", header)
            .csv(path.getCanonicalPath)
          checkAnswer(readback, df)
        }
      }
    }
  }



// COMMAND ----------

