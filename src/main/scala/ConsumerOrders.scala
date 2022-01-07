import org.apache.spark._
import org.apache
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.{StringToAttributeConversionHelper, booleanToLiteral}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object ConsumerOrders {
 def compareDf(x: Double, y: Double, z: Double): Double = {
  List(x, y, z).max
  //val lists=List(x,y,z)
  //lists.indexOf(1) > lists.indexOf(1) >lists.indexOf(1)
 }

 case class Categories(area_id: String, f_eggs_weight: Double, f_fruit_veg_weight: Double, f_grains_weight: Double)



 def monthly_data_selection(a: String, b: String, c: String): Unit = {
  val conf = new SparkConf().setMaster("local[*]").setAppName("ConsumerOrders")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext
  val absPath = "D:\\NaviKenz\\resources\\"
  val lsoaPath = "_lsoa_grocery.csv"
  val msoaPath = "_msoa_grocery.csv"

  val dfa = spark.read.option("header", "true").csv(absPath + a + lsoaPath, absPath + a + msoaPath)
  // dfa.show()
  val dfb = spark.read.option("header", "true").csv(absPath + b + lsoaPath, absPath + b + msoaPath)
  //dfb.show()
  val dfc = spark.read.option("header", "true").csv(absPath + c + lsoaPath, absPath + c + msoaPath)
  // dfc.show()
  val lookupLosaMsoa = spark.read.option("header", "true").csv(absPath + "lookupLsoaMsoa.csv")

  val firstMonth = dfa.select("area_id", "f_eggs_weight", "f_fruit_veg_weight", "f_grains_weight", "num_transactions")
  firstMonth.createOrReplaceTempView("firstconsumerView")
  val secondMonth = dfb.select("area_id", "f_eggs_weight", "f_fruit_veg_weight", "f_grains_weight", "num_transactions")
  secondMonth.createOrReplaceTempView("secondconsumerView")
  val thirdMonth = dfc.select("area_id", "f_eggs_weight", "f_fruit_veg_weight", "f_grains_weight", "num_transactions")
  thirdMonth.createOrReplaceTempView("thirdconsumerView")


     //  1.sales of your selected products increase or  decrease (across all areas)

  val result = spark.sql("select  SUM(f_eggs_weight) as Sum_f_eggs_weight,SUM(f_fruit_veg_weight) as Sum_f_fruit_veg_weight,SUM(f_grains_weight) as Sum_f_grains_weight from firstconsumerView");


  val result1 = spark.sql("select  SUM(f_eggs_weight) as Sum_f_eggs_weight,SUM(f_fruit_veg_weight) as Sum_f_fruit_veg_weight,SUM(f_grains_weight) as Sum_f_grains_weight from secondconsumerView");
  result1.show()

  val result2 = spark.sql("select  SUM(f_eggs_weight) as Sum_f_eggs_weight,SUM(f_fruit_veg_weight) as Sum_f_fruit_veg_weight,SUM(f_grains_weight) as Sum_f_grains_weight from thirdconsumerView");
  result2.show()
  val l3 = result.first.toSeq.asInstanceOf[Seq[Double]]
  val l4 = result1.first.toSeq.asInstanceOf[Seq[Double]]
  val l5 = result2.first.toSeq.asInstanceOf[Seq[Double]]


  val sale1 = compareDf(l3(0), l4(0), l5(0))
  val sale2 = compareDf(l3(1), l4(1), l5(1))
  val sale3 = compareDf(l3(2), l4(2), l5(2))

  val highest_sales_first = if (l3(0) > l4(0) && l3(0) > l5(0)) l3(0) + "Jan month sale increases" else if (l4(0) > l3(0) && l4(0) > l5(0)) l5(0) + "Feb month sale increases" else l5(0) + "march month sale increases"
  val highest_sales_second = if (l3(1) > l4(1) && l3(1) > l5(1)) l3(0) + "Jan month sale increases" else if (l4(1) > l3(1) && l4(1) > l5(1)) l5(0) + "Feb month sale increases" else l5(0) + "march month sale increases"
  val highest_sales_third = if (l3(2) > l4(2) && l3(2) > l5(2)) l3(0) + "Jan month sale increases" else if (l4(2) > l3(2) && l4(2) > l5(2)) l5(0) + "Feb month sale increases" else l5(0) + "march month sale increases"
  println("sales of first product" + highest_sales_first)



     //2.  MSOA had the highest sales for the specific product type(s) for each month?

  val msoaJanDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(absPath + a + msoaPath)
  val msoaFebDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(absPath + b + msoaPath)
  val msoaMarJanDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(absPath + c + msoaPath)
  //msoaDF.groupBy("area_id").max("f_eggs_weight","f_fruit_veg_weight","f_grains_weight").show()
  //msoaDF.select($"area_id",$max("f_eggs_weight"),max("f_fruit_veg_weight"),max("f_grains_weight"))
  val janMonth = msoaJanDF.select("area_id", "f_eggs_weight", "f_fruit_veg_weight", "f_grains_weight", "num_transactions")
  //janMonth.createOrReplaceTempView("EMP")
  val febMonth = msoaFebDF.select("area_id", "f_eggs_weight", "f_fruit_veg_weight", "f_grains_weight", "num_transactions")
  //febMonth.createOrReplaceTempView("DEPT")
  val marMonth = msoaMarJanDF.select("area_id", "f_eggs_weight", "f_fruit_veg_weight", "f_grains_weight", "num_transactions")
 // marMonth.createOrReplaceTempView("ADD")

  /*val hisghtes_msoa_f_eggs_weight = spark.sql("select e.area_id, max(e.f_eggs_weight) from EMP e, DEPT d, ADD a " +
    "where e.area_id == d.area_id and e.area_id == a.area_id" GROUP BY(area_id));*/
  val dfmsoajan=janMonth.join (febMonth, Seq ("area_id", "f_eggs_weight","f_fruit_veg_weight","f_grains_weight"))
  val commulative=dfmsoajan.join(marMonth, Seq ("area_id", "f_eggs_weight","f_fruit_veg_weight","f_grains_weight"))

  val wmax=commulative.groupBy("area_id").max("f_eggs_weight")
    //al umax= wmax.select("area_id","f_eggs_weight").sort("f_eggs_weight")
       val umax_eggs_weight=wmax.collect()
  println(" the highest sales area for f_eggs_weight" + umax_eggs_weight(0))

  val wmax_veg=commulative.groupBy("area_id").max("f_fruit_veg_weight")
  //al umax= wmax.select("area_id","f_eggs_weight").sort("f_eggs_weight")
  val umax_wmax_veg=wmax_veg.collect()
  println(" the highest sales area for f_eggs_weight" + umax_wmax_veg(0))

  val wmax_grains_weight=commulative.groupBy("area_id").max("f_grains_weight")
  //al umax= wmax.select("area_id","f_eggs_weight").sort("f_eggs_weight")
  val umax_wmax_grains=wmax_grains_weight.collect()
  println(" the highest sales area for f_eggs_weight" + umax_wmax_grains(0))
 /* val z=commulative.orderBy("area_id").sort("f_eggs_weight")//withColumn("area_id", max(col("f_eggs_weight")).as("Max_f_eggs_weight"))
   z.show()
  val y=commulative.orderBy("area_id").sort("f_fruit_veg_weight").as("fruit_veg_weight")//withColumn("area_id", max(col("f_eggs_weight")).as("Max_f_eggs_weight"))
  y.show()
  val x=commulative.groupBy("area_id").max("f_grains_weight").as("grains_weight")//withColumn("area_id", max(col("f_eggs_weight")).as("Max_f_eggs_weight"))
  x.show()*/

     //3. Within this MSOA, which LSOA contributed the highest sales for each of your product
  val lsoawithMsoa=lookupLosaMsoa.select("LSOA11CD","MSOA11CD","LSOA11NM","MSOA11NM","LAD15NM")
  lsoawithMsoa.createOrReplaceTempView("lsoaArea")
  val area_Msoa=  spark.sql("select LSOA11CD from lsoaArea WHERE NAME LIKE 'Eal%' ");

/*  def lsoaCollectedFormMsoa(row: Row) ={
     val lsoa_msoa=spark.sql("select LSOA11CD from lsoaArea WHERE  MSOA11CD = 'row' ");

  }*/
 // val total_grains_weight=  spark.sql("SELECT COALESCE ((SELECT sum(f_grains_weight) FROM  commulative WHERE  commulative.area_id = area_Richmond), 0) AS total_gains_weight");
  import spark.implicits._
  val collectLsoa=""
  val dis=lsoawithMsoa.select("MSOA11CD").distinct().map(f=>f.getString(0))
    .collect().toList
   for( i <- umax_eggs_weight){
    val row =umax_eggs_weight(0)
    if (dis.contains(row)){
     val collectLsoa =spark.sql("select LSOA11CD from lsoaArea WHERE  MSOA11CD = 'row' ");
    }

   }
  val finalLsoa_egg_weight =spark.sql("select sum(f_eggs_weight) from firstconsumerView WHERE  firstconsumerView.area_id = 'collectLsoa' ");
  val finalLsoa_veg_weight =spark.sql("select sum(f_fruit_veg_weight) from firstconsumerView WHERE  firstconsumerView.area_id = 'collectLsoa' ");
  val finalLsoa_grains_weight =spark.sql("select sum(f_grains_weight) from firstconsumerView WHERE  firstconsumerView.area_id = 'collectLsoa' ");








  //4. How much was the total weight sold for each of your selected product type(s) for this
     //highest selling MSOA?
  val  totalweight_eggs_weight =commulative.groupBy("area_id").sum("f_eggs_weight")
  val totalweight_fruit_veg_weight =commulative.groupBy("area_id").sum("f_fruit_veg_weight")
  val  totalweight_grains_weight=commulative.groupBy("area_id").sum("f_grains_weight")
  /*val commulativeCategories= (commulative.agg(max(commulative(commulative.columns(2)))),commulative.columns(1))
  println(commulativeCategories)
  janMonth.join(febMonth,janMonth("area_id") === febMonth("area_id"),"inner" )
    .join(marMonth,janMonth("area_id") === marMonth("area_id"),"inner")
    .show(false)*/
 // hisghtes_msoa_f_eggs_weight.show()
      /*val CategoriesSchema = new StructType()
   .add("area_id",StringType)
   .add("f_eggs_weight",DoubleType )
   .add("f_fruit_veg_weight",DoubleType )
    .add("f_grains_weight",DoubleType )*/

  /*import spark.implicits._
  val categoryDS=spark.read.schema(CategoriesSchema).csv(absPath+ a + msoaPath).as("msoaData")
   val m1= categoryDS.groupBy("area_id").agg(sum("f_eggs_weight")).as("f_eggs_weight").sort().show()*/
  /* val data = sc.textFile(absPath + a + msoaPath) //Location of the data file

   .map(line => line.split(","))

   .map(userRecord => (userRecord(0),

     userRecord(1), userRecord(2), userRecord(3)))

 val usersByProfession = data.map { case (area_id, f_eggs_weight, f_fruit_veg_weight, f_grains_weight) => (f_eggs_weight,1) }
   // .reduceByKey(_ + _).sortBy(-_._2)
   //.reduceByKey(x,y).sortBy(-_._2)(-_._3)
   .groupBy("area_id")
 usersByProfession.collect().foreach(println)*/

  /*val msoadata=msoaDF.select("area_id", "f_eggs_weight", "f_fruit_veg_weight", "f_grains_weight", "num_transactions").rdd.max()
 //msoadata.createOrReplaceTempView("msoaView")
 //select *from employees where salary=(select max(salary) from employees);
 val jan_fruit_veg_weight=spark.sql("select *  FROM msoaView where f_fruit_veg_weight=(select max(f_fruit_veg_weight) from msoaView)");

 res1.show()*/
  /*val msoadata = msoaDF.select("area_id", "f_eggs_weight", "f_fruit_veg_weight", "f_grains_weight", "num_transactions")
  // Find first record of the file
  val msoaRDD = msoadata.rdd
  // Find first record of the file
  val header = msoaRDD1.first()
  println(header)

  // Remove header from RDD
  val data_without_header = msoaRDD1.filter(line => !line.equals(header))
  //println(data_without_header.foreach(println))

  // Using max function on RDD
  val emp_salary_list = data_without_header.map { x => x.split(',') }.map { x => (x(177).toDouble) }
  println("Highest salaty:" + emp_salary_list.max())

  // Employee who have max salary
  val salaryWithEmployeeName = data_without_header.map { x => x.split(',') }.map { x=> (x(177).toDouble, x(1)) }
  val maxSalaryEmployee = salaryWithEmployeeName.groupByKey.takeOrdered(2)(Ordering[Double].reverse.on(_._1))*/
  //println(maxSalaryEmployee.foreach(println))
  /*result1.first.toSeq.map{
case x: String => x.toDouble
case x: Double => x
}*/
   //5.Which LSOA within Ealing sold the most fruits/vegetables within these 3 months?


  val area_Ealing=  spark.sql("select LSOA11CD from lsoaArea WHERE NAME LIKE 'Eal%' ");

  val area_Ealing_highest_egg=  spark.sql("SELECT area_id,COALESCE ((SELECT Max(f_eggs_weight) FROM  firstconsumerView WHERE  firstconsumerView.area_id = area_Ealing), 0) AS areas");
  val area_Ealing_highest_veg=  spark.sql("SELECT area_id,COALESCE ((SELECT Max(f_fruit_veg_weight) FROM  firstconsumerView WHERE  firstconsumerView.area_id = area_Ealing), 0) AS areas");
  val area_Ealing_highest_grains=  spark.sql("SELECT area_id,COALESCE ((SELECT Max(f_grains_weight) FROM  firstconsumerView WHERE  firstconsumerView.area_id = area_Ealing), 0) AS areas");


  //6. What was the total weight of grains sold in all MSOAs in Richmond upon Thames
  val area_Richmond=  spark.sql("select MSOA11CD from lsoaArea WHERE NAME LIKE 'Richmond%' ");
  val total_grains_weight=  spark.sql("SELECT COALESCE ((SELECT sum(f_grains_weight) FROM  commulative WHERE  commulative.area_id = area_Richmond), 0) AS total_gains_weight");
 }

}

