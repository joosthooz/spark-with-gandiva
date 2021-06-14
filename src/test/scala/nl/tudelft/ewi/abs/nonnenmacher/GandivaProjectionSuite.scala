package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.{ArrowColumnarExtension, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}


@RunWith(classOf[JUnitRunner])
class GandivaProjectionSuite extends FunSuite with SparkSessionBuilder{

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(ProjectionOnGandivaExtension(), ArrowColumnarExtension())

  ignore("that a simple addition query can be executed on Gandiva") {

    // Deactivates whole stage codegen, helpful for debugging
    // spark.conf.set("spark.sql.codegen.wholeStage", false)

    import spark.implicits._

    val tuples = List((1, 1, 1, 1),
      (2, 2, 2, 2),
      (3, 3, 3, 3),
      (4, 4, 4, 4),
      (5, 5, 5, 5),
      (6, 6, 6, 6))

    val df = spark.createDataset(tuples)
      .toDF("a", "b", "c", "d")
      .repartition(2) // Enforces a separate Projection step
    // otherwise Spark optimizes the projection and combines it with the data generation

    val res = df.select(col("a") + col("b") + col("c"))//.where(col("a") < 100)

    println("Logical Plan:")
    println(res.queryExecution.optimizedPlan)
    println("Spark Plan:")
    println(res.queryExecution.sparkPlan)
    println("Executed Plan:")
    println(res.queryExecution.executedPlan)

    // Shows generated code, helpful for debugging
    // println(res.queryExecution.debug.codegen())
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[GandivaProjectExec]).isDefined)

    //Verify the expected results are correct
    val results: Array[Int] = res.collect().map(_.getInt(0));
//    assert(results.length == 6)
    println("Result: " + results.toSet)
//    assert(Set(12, 32, 60, 96, 140, 192).subsetOf(results.toSet))
  }

  test("Add 3 columns, Gandiva") {

    import spark.implicits._

    var totalextimes = 0.0
	var lastextime = 0.0
    var i=0
    val nIters = 4
    for (i <- 0 to nIters - 1)  {
	    val tc1 = System.nanoTime()
//        val dfin = spark.range(5 * 1e6.toLong).rdd.map(x => (x + 1.0, 5 * 1e6 - x, x / 2)).toDF("a", "b", "c")
        val dfin = spark.range(5 * 1e6.toLong).rdd.map(x => (1, 2, 3)).toDF("a", "b", "c")
	    val tc2 = System.nanoTime()
	    println("Input creation time: " + (tc2 - tc1)/1e6 + "ms")

//        val dfin = spark.read.parquet("/home/jjhoozemans/workspaces/fletcher/datasets/parquet/d500")
	    val te1 = System.nanoTime()
//        //val df = dfin.select(max(col("a") + col("b") + col("c"))).first()
        //val row = spark.sql(s"SELECT `a` + `b` + `c` AS sum FROM parquet.`/home/jjhoozemans/workspaces/fletcher/datasets/parquet/d500`").agg("sum" -> "max").first()
        val df = dfin.select(col("a") + col("b") + col("c"))
        val results = df.collect() //collect() is a shitty way to force evaluation, it also copies data!
	    val te2 = System.nanoTime()
        
        totalextimes += (te2 - te1)/1e6
		lastextime = (te2 - te1)/1e6
	    //println("Gandiva execution time: " + (te2 - te1)/1e6 + "ms for " + dfin.count + "rows ("+ (dfin.count * 3 * 8 / 1e6) + " Mbytes), " + (dfin.count * 3 * 8 / ((te2 - te1)/1e3)) + "MB/s")
        println("Gandiva execution time: " + (te2 - te1)/1e6 + "ms")
    }

    val meanextimes = totalextimes / nIters
    println("Gandiva mean execution time: " + meanextimes)
//    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[GandivaProjectExec]).isDefined)

//    df.take(10).foreach(println(_))
//    println(df.count())
  }
}
