import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{AddFields, DropFields, Expression, ExpressionDescription, ExpressionInfo, RenameFields, RuntimeReplaceable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{FunSuite, Matchers}

import scala.reflect.ClassTag

class SqlExampleTest extends FunSuite with Matchers {

  /**
   * Creates an [[ExpressionInfo]] for the function as defined by expression T using the given name.
   * Copied from Apache Spark project: https://github.com/apache/spark/
   */
  private def expressionInfo[T <: Expression : ClassTag](name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName,
          null,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          df.since(),
          df.deprecated())
      } else {
        // This exists for the backward compatibility with old `ExpressionDescription`s defining
        // the extended description in `extended()`.
        new ExpressionInfo(clazz.getCanonicalName, null, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }

  /**
   * See usage above.
   * * Copied from Apache Spark project: https://github.com/apache/spark/
   * */
  private def expression[T <: Expression](name: String)
                                         (implicit tag: ClassTag[T]): (String, (ExpressionInfo, FunctionBuilder)) = {

    // For `RuntimeReplaceable`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `child` and should not be used as function builder.
    val constructors = if (classOf[RuntimeReplaceable].isAssignableFrom(tag.runtimeClass)) {
      val all = tag.runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      tag.runtimeClass.getConstructors
    }
    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        try {
          varargCtor.get.newInstance(expressions).asInstanceOf[Expression]
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new RuntimeException(e.getCause.getMessage)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
            .map(_.getParameterCount).distinct.sorted
          val invalidArgumentsMsg = if (validParametersCount.length == 0) {
            s"Invalid arguments for function $name"
          } else {
            val expectedNumberOfParameters = if (validParametersCount.length == 1) {
              validParametersCount.head.toString
            } else {
              validParametersCount.init.mkString("one of ", ", ", " and ") +
                validParametersCount.last
            }
            s"Invalid number of arguments for function $name. " +
              s"Expected: $expectedNumberOfParameters; Found: ${params.length}"
          }
          throw new RuntimeException(invalidArgumentsMsg)
        }
        try {
          f.newInstance(expressions: _*).asInstanceOf[Expression]
        } catch {
          // the exception is an invocation exception. To get a meaningful message, we need the
          // cause.
          case e: Exception => throw new RuntimeException(e.getCause.getMessage)
        }
      }
    }

    (name, (expressionInfo[T](name), builder))
  }


  val addFieldsFunction: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val temp = expression[AddFields]("add_fields")
    (FunctionIdentifier(temp._1), temp._2._1, temp._2._2)
  }

  val dropFieldsFunction: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val temp = expression[DropFields]("drop_fields")
    (FunctionIdentifier(temp._1), temp._2._1, temp._2._2)
  }

  val renameFieldsFunction: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val temp = expression[RenameFields]("rename_fields")
    (FunctionIdentifier(temp._1), temp._2._1, temp._2._2)
  }

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("test")
    .withExtensions { extensions =>
      extensions.injectFunction(addFieldsFunction)
      extensions.injectFunction(dropFieldsFunction)
      extensions.injectFunction(renameFieldsFunction)
    }
    .getOrCreate()

  val sparkContext: SparkContext = spark.sparkContext

  sparkContext.setLogLevel("ERROR")

  val structLevel1: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Row(Row(1, 2, 3)) :: Nil),
    StructType(Seq(StructField("a",
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType))))))).cache

  test("add new field to struct") {
    val result = structLevel1.withColumn("a", expr("add_fields(a, 'd', 4)"))

    result.collect should contain theSameElementsAs Row(Row(1, 2, 3, 4)) :: Nil
    result.schema shouldEqual StructType(Seq(StructField("a",
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType),
        StructField("d", IntegerType, nullable = false))))))
  }

  test("drop field in struct") {
    val result = structLevel1.withColumn("a", expr("drop_fields(a, 'b')"))

    result.collect should contain theSameElementsAs Row(Row(1, 3)) :: Nil
    result.schema shouldEqual StructType(Seq(StructField("a",
      StructType(Seq(
        StructField("a", IntegerType),
        StructField("c", IntegerType))))))
  }

  test("rename field in struct") {
    val result = structLevel1.withColumn("a", expr("rename_fields(a, 'a', 'x')"))

    result.collect should contain theSameElementsAs Row(Row(1, 2, 3)) :: Nil
    result.schema shouldEqual StructType(Seq(StructField("a",
      StructType(Seq(
        StructField("x", IntegerType),
        StructField("b", IntegerType),
        StructField("c", IntegerType))))))
  }
}