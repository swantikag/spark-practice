import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Main extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkPractice")
    .getOrCreate()

  /*val df = spark
    .read
    .options(Map("header" -> "true"))
    .csv("/home/knoldus/Desktop/spacedomains3.csv")
  df.show(5)

  //array()
  df.printSchema()*/

  //  val df2 = df.select(array(df.columns.map(col):_*))
  //
  //  df2.printSchema()

  /*val dfGrouped = df.groupBy(col("sensorLocalDate"))
    .agg(collect_list(col("array_sn")).as("array_list"))
  dfGrouped.show(false)

  val array_contains_df = dfGrouped
    .withColumn("result", array_contains(col("array_list"), "AF-181194"))
  array_contains_df.show(false)

  val array_appended_df = dfGrouped
    .select(array_append(col("array_list"), null))
  array_appended_df.show(false)

  val array_compact_df = array_appended_df
    .select(array_compact(col("array_append(array_list, NULL)")))
  array_compact_df.show(false)

  val array_distinct_df = dfGrouped
    .select(array_append(col("array_list"), "AF-226996").as("array_list"))
    .select(array_distinct(col("array_list")))
  array_distinct_df.show(false)

  val array_element_at = dfGrouped
    .select(element_at(col("array_list"), 2))
  array_element_at.show(false)*/


  import spark.implicits._
/*

  val columns = Seq("Seqno", "Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy.")
  )
  val quotesDf = data.toDF(columns: _*)

  val convertCase = (strQuote: String) => {
    val arr = strQuote.split(" ")
    arr.map(f => f.substring(0, 1).toUpperCase + f.substring(1, f.length)).mkString(" ")
  }

  val convertUDF = udf(convertCase) //registering the function as UDF

  quotesDf.select(col("Seqno"),
    convertUDF(col("Quote")).as("Quote"))
    .show(false)


  // Using it on SQL
  spark.udf.register("convertUDF", convertCase)
  quotesDf.createOrReplaceTempView("QUOTE_TABLE")
  spark.sql("select Seqno, convertUDF(Quote) from QUOTE_TABLE")
    .show(false)
*/


  /*val columns = Seq("Seqno", "Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy.")
  )
  val quotesDf: DataFrame = data.toDF(columns: _*) // data.toDF("SeqNo", "Quote")

  quotesDf.createOrReplaceGlobalTempView("QUOTES")

  spark.sql("""select * from global_temp.QUOTES""")
    .show(false)*/

  /*val simpleData: Seq[(String, String, Int, String)] = Seq(
    ("James", "Sales", 3000, "Delhi"), //1
    ("Michael", "Sales", 4600, "Gurugram"), //2
    ("Robert", "Sales", 4100, "Noida"), //3
    ("Maria", "Finance", 3000, "Delhi"), //4
    ("James", "Sales", 3000, "Gurugram"), // negate
    ("Scott", "Finance", 3300, "Noida"), //5
    ("Jen", "Finance", 3900, "Gurugram"), //6
    ("Jeff", "Marketing", 3000, "Delhi"), //7
    ("Kumar", "Marketing", 2000, "Noida"), //8
    ("Saif", "Sales", 4100, "Delhi") //negate
  )
  val df = simpleData.toDF("employee_name", "department", "salary", "location")
  df.show()*/

  // ----------------------- Aggregate Functions -------------------------

  /*println("approx_count_distinct: ")
  df.select(approx_count_distinct("salary"))
    .show(false)

  println("avg: ")
  df.select(avg("salary"))
    .show(false)

  println("collect_list: ")
  df.select(collect_list("salary"))
    .show(false)

  println("collect_set: ")
  df.select(collect_set("salary"))
    .show(false)

  println("countDistinct: ")
  df.select(countDistinct("department", "salary"))
    .show(false)

  println("count: ")
  df.select(count("salary"))
    .show(false)

  println("first: ")
  df.select(first("salary")).show(false)

  println("last: ")
  df.select(last("salary")).show(false)

  println("sum: ")
  df.select(sum("salary")).show(false)

  println("sumDistinct: ")
  df.select(sum_distinct(col("salary"))).show(false)

  println("average salaries per department")
  df
    .groupBy("department")
    .avg("salary")
    .show(false)

  df
    .groupBy("department")
    .min("salary")
    .show(false)

  df
    .groupBy("department")
    .max("salary")
    .show(false)

  df
    .groupBy("department")
    .sum("salary")
    .show(false)

  df
    .groupBy("location","department")
    .avg("salary")
    .show(false)

  df
    .groupBy("department")
    .agg(
      sum("salary").as("sum_salary"),
      avg("salary").as("avg_salary"),
      min("salary").as("min_salary"),
      max("salary").as("max_salary")
    )
    .show(false)*/

  /*df
    .groupBy("department")
    .agg(
      sum("salary").as("sum_salary"),
      avg("salary").as("avg_salary"),
      min("salary").as("min_salary"),
      max("salary").as("max_salary")
    )
    .where(col("avg_salary") >= 3000)
    .select("department")
    .show(false)*/

  // ---------------- Window Functions --------------
  //row_number
//  val windowSpec = Window.partitionBy("department").orderBy("salary")
  //  val windowSpec = Window.partitionBy("department").orderBy(col("salary").desc) // If ordering on the basis of descending salary
  /*df.withColumn("row_number", row_number.over(windowSpec))
    .show()

  //rank
  df.withColumn("rank", rank().over(windowSpec))
    .show()

  //dens_rank
  df.withColumn("dense_rank", dense_rank().over(windowSpec))
    .show()*/

  //lag and lead
  /*val windowSpecLagLead = Window.partitionBy("location").orderBy("salary")
  df.withColumn("lag", lag("salary", 2).over(windowSpecLagLead))
    .withColumn("lead", lead("salary", 1).over(windowSpecLagLead))
    .show()*/

  //For each department, calculate the stats of top salary only
  /*val windowSpecAgg = Window.partitionBy("location", "department")
  val df_stats = df
    .withColumn("row_number", row_number().over(windowSpecAgg.orderBy(col("salary").desc)))
    .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
    .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
    .withColumn("min", min(col("salary")).over(windowSpecAgg))
    .withColumn("max", max(col("salary")).over(windowSpecAgg))

  df_stats.show(false)

  df_stats
    .where(col("row_number") === 1)
//    .select("department", "avg", "sum", "min", "max")
    .show(false)*/


  //For each department, show details of the employee with top salary

  /*val rankedDf = df
    .withColumn("row_number", row_number()
      .over(Window.partitionBy("department").orderBy(col("salary").desc)))

    rankedDf.show(false)

    rankedDf
    .where(col("row_number") === 1)
    .show(false)*/

  /*val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
    (2, "Rose", 1, "2010", "20", "M", 4000),
    (3, "Williams", 1, "2010", "10", "M", 1000),
    (4, "Jones", 2, "2005", "10", "F", 2000),
    (5, "Brown", 2, "2010", "40", "", -1),
    (6, "Brown", 2, "2010", "50", "", -1)
  )
  val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
    "emp_dept_id", "gender", "salary")

  val empDF = emp.toDF(empColumns: _*)

  empDF.show(false)

  val dept = Seq(("Finance", 10),
    ("Marketing", 20),
    ("Sales", 30),
    ("IT", 40)
  )

  val deptColumns = Seq("dept_name", "dept_id")
  val deptDF = dept.toDF(deptColumns: _*)

  deptDF.show(false)


  /*
  1) join(right: Dataset[_]): DataFrame
  2) join(right: Dataset[_], usingColumn: String): DataFrame
  3) join(right: Dataset[_], usingColumns: Seq[String]): DataFrame
  4) join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame
  5) join(right: Dataset[_], joinExprs: Column): DataFrame
  6) join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame
   */

  println(" Cross Join: ===>")

  empDF.join(deptDF)
  .show(false)//wrong for this case as no column has same name

  println("Inner Join: ===>")
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner")
    .show(false)

  println("Outer Join ====>")
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "outer")
    .show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full")
    .show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter")
    .show(false)


  println(s"Left Outer Join ===>")

  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left")
    .show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftouter")
    .show(false)


  println("Right Outer Join ===>")

  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "right")
    .show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "rightouter")
    .show(false)

  println(" Left Semi Join ====>")

  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftsemi")
    .show(false)

  println("Left Anti Join ===>")

  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftanti")
    .show(false)

  println("Self Join ===>")

  empDF.as("emp1")
    .join(empDF.as("emp2"),
    col("emp1.superior_emp_id") === col("emp2.emp_id"), "inner")
    .select(col("emp1.emp_id"), col("emp1.name"),
      col("emp2.emp_id").as("superior_emp_id"),
      col("emp2.name").as("superior_emp_name"))
    .show(false)*/

  val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
    (2, "Rose", 1, "2010", "20", "M", 4000),
    (3, "Williams", 1, "2010", "10", "M", 1000),
    (4, "Jones", 2, "2005", "10", "F", 2000),
    (5, "Brown", 2, "2010", "40", "", -1),
    (6, "Brown", 2, "2010", "50", "", -1)
  )
  val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
    "dept_id", "gender", "salary")

  val empDF = emp.toDF(empColumns: _*)

  empDF.show(false)

  val dept = Seq(("Finance", 10),
    ("Marketing", 20),
    ("Sales", 30),
    ("IT", 40)
  )

  val deptColumns = Seq("dept_name", "dept_id")
  val deptDF = dept.toDF(deptColumns: _*)

  empDF.join(deptDF, "dept_id")
    .show(false)

}
