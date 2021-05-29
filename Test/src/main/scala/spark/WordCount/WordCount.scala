package spark.WordCount


import org.apache.spark.{SparkConf, SparkContext}

/**
 * @create 2019-11-15 21:41
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //指明hadoop文件的路径
    //System.setProperty("hadoop.home.dir", "D:/Program Files/hadoop-2.6.4")
    //创建SparkConf对象
    //设定Spark计算框架的运行环境
    //appid
    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //读取文件,将文件内容按行读取
    val lines = sc.textFile("input/word.txt")
    //将数据分解成单词
    val words = lines.flatMap(_.split(" "))
    //为了方便统计，将单词数据进行结构转换
    val wordToOne = words.map(((_,1)))
    //对转换后的数据进行分组聚合,并输出打印
    val wordToSum = wordToOne.reduceByKey(_+_).collect()
    //打印结果
    wordToSum.foreach(println)

  }
}
