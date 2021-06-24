package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import scala.io.Source
object SparkObj_san {
  	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark=SparkSession.builder().getOrCreate()
					import spark.implicits._

					val df = spark.read.format("json").option("multiLine","true").load("file:///F:/Data/array2.json")
					df.printSchema()
					df.show()

					println("==========Flatten Data==========")
					val flattendf = df.select(
							"Students",
							"address.Permanent_address",
							"address.temporary_address",
							"first_name",
							"second_name"
							)

					val explodf = flattendf.withColumn("Students", explode( col("Students")))
					explodf.show()
					explodf.printSchema()
					val comexplode = explodf.withColumn("Components",explode(col("Students.user.components")))
					comexplode.show()
					comexplode.printSchema()
					println("==========Final Flatten Data==========")
					val finaldf = comexplode.select(
							col("Students.user.address.Permanent_address").alias("cPermanent_address"),
							col("Students.user.address.temporary_address").alias("cTemporary_address"),
							col("Students.user.gender"),
							col("Students.user.name.first").alias("first_name"),
							col("Students.user.name.last").alias("last_name"),
							col("Students.user.name.title").alias("title"),
							col("Permanent_address"),
							col("temporary_address"),
							col("first_name"),
							col("second_name"),
							col("components"))
					finaldf.show()
					finaldf.printSchema()
					


	}
  
}