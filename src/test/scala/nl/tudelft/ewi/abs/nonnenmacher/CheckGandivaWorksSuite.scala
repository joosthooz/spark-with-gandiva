package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.gandiva.evaluator.Projector
import org.apache.arrow.gandiva.expression.ExpressionTree
import org.apache.arrow.gandiva.expression.TreeBuilder.{makeExpression, makeField, makeFunction}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.arrow.vector._
import org.apache.spark.sql.util.ArrowUtils
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CheckGandivaWorksSuite extends FunSuite {

  lazy val testAllocator: BufferAllocator = {
    ArrowUtils.rootAllocator.newChildAllocator("SparkTest", 0, Long.MaxValue)
  }

  ignore("that all Gandiva dependencies are set-up correctly and it can execute a projection") {
    // In this test basically no code from me is executed and it just checks Gandiva

    // arrow field definition of input
    val in1: Field = Field.nullable("in1", MinorType.INT.getType)
    val in2: Field = Field.nullable("in2", MinorType.INT.getType)

    // arrow field definition of output
    val out: Field = Field.nullable("out", MinorType.INT.getType)

    // defining the gandiva expression, which should eb executed on Gandiva
    val gandivaExpression: ExpressionTree = makeExpression(makeFunction("add", List(makeField(in1), makeField(in2)).asJava, MinorType.INT.getType), out)

    // initializing the gandiva projection
    val inputSchema: Schema = new Schema(List(in1, in2).asJava)
    val eval: Projector = Projector.make(inputSchema, List(gandivaExpression).asJava)

    // arrow input data - field names 
    val v1 = integerVector(in1, 1,2,3)
    val v2: IntVector = integerVector(in2, 10, 20, 30)

    // convert input data into record batch
    val root = new VectorSchemaRoot(Seq(v1.asInstanceOf[FieldVector], v2).asJava)
    root.setRowCount(3)
    val recordBatch: ArrowRecordBatch = new VectorUnloader(root).getRecordBatch

    // allocate memroy for the output vector
    val outVector = new IntVector("out", testAllocator)
    outVector.allocateNew(root.getRowCount)
    val outVectors: List[ValueVector] = List(outVector)

    //execute Gandiva projection
    eval.evaluate(recordBatch, outVectors.asJava)

    // verify output data
    assert(outVector.get(0) == 11)
    assert(outVector.get(1) == 22)
    assert(outVector.get(2) == 33)
    println(outVector.get(0))
  }

  private def integerVector(field: Field, values: Int*) = {
    val vector = new IntVector(field, testAllocator)
    vector.allocateNew(values.size)
    values.zipWithIndex.foreach{ case (v, i) =>
          vector.set(i, v)
    }
    vector
  }
}
