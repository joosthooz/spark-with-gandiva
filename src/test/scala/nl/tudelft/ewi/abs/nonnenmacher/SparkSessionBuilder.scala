package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait SparkSessionBuilder extends BeforeAndAfterAll with BeforeAndAfterEach  {
  this: Suite =>

  lazy val testAllocator: BufferAllocator = {
    ArrowUtils.rootAllocator.newChildAllocator("SparkTest", 0, Long.MaxValue)
  }

  lazy val spark: SparkSession = {
    val builder = SparkSession
      .builder();

    withExtensions.foreach { extension =>
      builder.withExtensions(extension)
    }

    builder.appName(this.styleName)
      .config("spark.master", "local")
      .getOrCreate()
  }

  def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq()

  override def afterAll(): Unit = {
    spark.close()
  }

  // Close and delete the temp file
  override def afterEach() {
    //Check that all previously allocated memory is released
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == 0)
    assert(testAllocator.getAllocatedMemory == 0)
  }
}
