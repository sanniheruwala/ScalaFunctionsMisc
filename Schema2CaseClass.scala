/**
  * Generate Case class from DataFrame.schema
  *
  * val df:DataFrame = ...
  *
  * val s2cc = new Schema2CaseClass
  * import s2cc.implicits._
  *
  * println(s2cc.schemaToCaseClass(df.schema, "MyClass"))
  *
  *
  * import scala.reflect.runtime.universe._
  * import scala.reflect.runtime.currentMirror
  * import scala.tools.reflect.ToolBox
  * val toolbox = currentMirror.mkToolBox()
  // write your code starting with q  and put it inside double quotes.
  // NOTE : you will have to use triple quotes if you have any double quotes usage in your code.
  * val str = s2cc.schemaToCaseClass(df.schema, "de9_asset_pd")
  * val code1 = q"${str}"
  //compile and run your code.
  * val result1 = toolbox.compile(code1)
  *
  */

import org.apache.spark.sql.types._

class Schema2CaseClass {
  type TypeConverter = (DataType) => String

  def schemaToCaseClass(schema: StructType, className: String)(implicit tc: TypeConverter): String = {
    def genField(s: StructField): String = {
      val f = tc(s.dataType)
      s match {
        //case x if (x.nullable) => s"  ${s.name}:Option[$f]"
        case _ => s"""  ${s.name}:$f"""
      }
    }

    val fieldsStr = schema.map(genField).mkString(",")
    s"""case class $className ($fieldsStr)""".stripMargin
  }

  object implicits {
    implicit val defaultTypeConverter: TypeConverter = (t: DataType) => {
      t match {
        case _: ByteType => "Byte"
        case _: ShortType => "Short"
        case _: IntegerType => "Int"
        case _: LongType => "Long"
        case _: FloatType => "Float"
        case _: DoubleType => "Double"
        case _: DecimalType => "java.math.BigDecimal"
        case _: StringType => "String"
        case _: BinaryType => "Array[Byte]"
        case _: BooleanType => "Boolean"
        case _: TimestampType => "java.sql.Timestamp"
        case _: DateType => "java.sql.Date"
        case _: ArrayType => "scala.collection.Seq"
        case _: MapType => "scala.collection.Map"
        case _: StructType => "org.apache.spark.sql.Row"
        case _ => "String"
      }
    }
  }

}