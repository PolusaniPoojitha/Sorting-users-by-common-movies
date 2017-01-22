import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object qqq{
  def main(args: Array[String]): Unit = {
    if (args.length < 3){
        System.err.println("Usage: q1 <input file>  <output file>")
        System.exit(1);  
      }
val conf = new SparkConf().setMaster("yarn").setAppName("Assignment6");
val sc = new SparkContext(conf);
conf.set("spark.sql.warehouse.dir","hdfs://Hadoop-Ravi-Pujitha:9000/user/hive/warehouse")
val lines = sc.textFile(args(0))
case class movies(movieId:String,movieTitle:String)
case class ratings(UserId:String,movieId:String)
var ratingsDF=sc.textFile(args[0]).map(x=>(x.split("\\s+")(0),x.split("\\s+")(1).split("#"))).flatMapValues(x=>x).map{case(x,y)=>ratings(x,y)}.toDF
var moviesDF=sc.textFile(args[1]).map(x=>(movies(x.split("#")(0),x.split("#")(1)))).toDF()
ratingsDF.createOrReplaceTempView("user_movies")
moviesDF.createOrReplaceTempView("movies")
//spark.conf.set("spark.sql.crossJoin.enabled", true)
sql("select usr1.userId,usr2.userId,count(usr1.movieId) as movieCount,collect_set(m.movieTitle) from user_movies usr1,user_movies usr2,movies m where usr1.movieId=usr2.movieId and m.movieId=usr1.movieId and usr1.userId<usr2.userId group by usr1.userId,usr2.userId Having moviesCount>50 ORDER BY movieCount desc").rdd.saveAsTextFile(args(2));
  }
}
