import org.apache.spark.{SparkContext, SparkConf}

object mycount {
 
  def main(args: Array[String])
  {
    val conf = new SparkConf()
    
    val spark = new SparkContext(conf)
    
    val input_file = spark.textFile("/data/2822/")    
    
    val counts = textFile.flatMap(line => line.split(",")(0)).reduceByKey(_)

    counts.saveAsTextFile("/user/chrixdou/pre_ids")
  }
}
