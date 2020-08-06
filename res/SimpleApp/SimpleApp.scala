import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.{Try, Success, Failure}
import scala.sys.process._

// Required starting from scala 2.13
// import scala.language.postfixOps

object SimpleApp {

  def makeInt(s: String): Try[Int] = Try(s.trim.toInt)

  def decode(input: String): Try[String] = {
    Try(new String(java.util.Base64.getDecoder().decode(input)))
  }

  def executeOnSpark(cmd: String, numTasks: Int) ={
    val sc = new SparkContext(new SparkConf())
    val myList = sc.parallelize(List.range(0, numTasks), numTasks)
    val output = myList.map( x => (s"echo -ne $cmd" #| "base64 -d" #| "bash")!!)
    output.collect().zipWithIndex.foreach{ case (e, i) => println(s"[+] worker $i\n$e\n=========") }
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      println("Need to pass command to execute")
      sys.exit
    }
    val numTasks = (args.lift(1) match {
      case Some(x:String) => (makeInt(x) match{
        case Success(x) => x
        case _ => 1
      })
      case _ => 1
    })
    executeOnSpark(args(0), numTasks)
  }
}
