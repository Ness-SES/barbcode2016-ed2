Start 3 spark workers on local machine
**************************************

export SPARK_WORKER_INSTANCES=3
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=4g

sbin/start-master.sh
./sbin/start-slave.sh spark://L3701100067:7077

########################################################

//start spark shell in cloudera-quickstar
bin/spark-shell --master local[4] --name "Tutorial" --executor-memory 6G --total-executor-cores 4


def printArray(a: Array[String]) = println(a.mkString(" "))

//read file
val lines = sc.textFile("file:///Users/p3700698/100meters.txt")
val usainLines = lines.filter(line => line contains "Usain")
usainLines.count()

//list to rdd
val lines = sc.parallelize(List("you", "shall", "not pass"))
val newRDD = lines.filter(word => word.equals("you"))


############################################################


//find all 100 meter competition cities from 2010
val lines =sc.textFile("file:///Users/p3700698/100meters.txt")
val splitLines = lines.map(row => row.split("\\s{2,}"))
val lines2010 = splitLines.filter(x => x(x.length-1).split("\\.")(2).equals("2010"))



//Get all the winners holder names
val lines =sc.textFile("file:///Users/p3700698/100meters.txt")
val splitLines = lines.map(row => row.split("\\s{2,}"))
val names = splitLines.map(row => row(3) + " " + row(4)).distinct()


//Print out all possible pairs of sprinter names. (sprinter1, sprinter2)
val lines =sc.textFile("file:///Users/p3700698/100meters.txt")
val splitLines = lines.map(row => row.split("\\s{2,}"))
val names = splitLines.map(row => row(2)).distinct()
val namesCombination = names.cartesian(names).filter{case(name1,name2) => !name1.equals(name2)}


//Print out the sum of all winning sprint times
val lines =sc.textFile("file:///Users/p3700698/100meters.txt")
val splitLines = lines.map(row => row.split("\\s{2,}"))
val times = splitLines.map(row => row(1).toDouble)
val sum = times.reduce{case(time1, time2) => time1 + time2}
println(sum)


#################################################################

//partitioner
val pairs = sc.parallelize(List((1, "Alice"), (2, "Bob"), (3, "Eve")))
pairs.partitioner
val partitionedPairs = pairs.partitionBy(new org.apache.spark.HashPartitioner(2))
partitionedPairs.partitioner





########################################################################

//Find out how many time Usain Bolt won every year.
val lines =sc.textFile("file:///Users/p3700698/100meters.txt")
val splitLines = lines.map(row => row.split("\\s{2,}"))
val namesYear = splitLines.map(row => Array(row(2), row(row.length-1).split("\\.")(2)))
val filtered = namesYear.filter(row => row(0).equals("Usain Bolt"))
filtered.map(row => ((row(0), row(1)), 1)).reduceByKey(_ + _).sortByKey(false).collect().foreach(println)


//Get the best sprint times Usain Bolt got for every year, ordered by year descending.
val lines =sc.textFile("file:///Users/p3700698/100meters.txt")
val splitLines = lines.map(row => row.split("\\s{2,}"))
val namesYear = splitLines.map(row => Array(row(2), row(1), row(row.length-1).split("\\.")(2)))
val filtered = namesYear.filter(row => row(0).equals("Usain Bolt"))
val keyValue = filtered.map(row => ((row(0), row(2)), row(1)))
keyValue.aggregateByKey((100.0))(
      {
        case ((localMin), time) =>
          (localMin min time.toDouble)
      }, 
      {
        case ((localMin1),(localMin2)) =>
          (localMin1 min localMin2)
      }
    ).sortByKey(false).collect().foreach(println)






//create DataFrame from RDD from case class
case class Person(name: String, age: Int)

val rdd = sc.parallelize(Array( Person("Alice", 25), Person("Bob", 23), Person("Eve", 29)))

val dataFrame = rdd.toDF()




//create DataFrame from RDD using reflection
val rdd = sc.parallelize(Array( ("Alice", 25), ("Bob", 23), ("Eve", 29)))

val dataFrame = rdd.toDF("name", "age")




//create DataFrame from RDD by adding schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val rdd = sc.parallelize(Array( ("Alice", 25), ("Bob", 23), ("Eve", 29))).map{case (t,y) => Row(t,y)}

val schema = StructType( List(
    StructField("name", StringType, false),
    StructField("age",  IntegerType, false)))

val dataFrame = sqlContext.createDataFrame(rdd, schema)



//print DataFrame schema
dataFrame.printSchema()
//get first row
val row = dataFrame.first()
//print first field of row
println(row(0))
//check if field is null
println(row.isNullAt(1))
//get field value
println(row.getInt(1))


//get complex types from Row
val rdd = sc.parallelize(Array( (Seq("Alice", "Smith"), 25), (Seq("Bob", "Taylor"), 23), (Seq("Eve", "Blunt"), 29)))
val dataFrame = rdd.toDF("names", "age")
var row = dataFrame.first
row(0).asInstanceOf[Seq[String]].foreach(println)


//Get the top 3 years where Usein participated the most the average time he scored
//year  #participations #avg time
val lines =sc.textFile("file:///Users/p3700698/100meters.txt")
val splitLines = lines.map(row => row.split("\\s{2,}"))
val splitLinesCorrected = splitLines.filter(row => row.length == 7)
val splitLinesTuple = splitLinesCorrected.map( x => (x(0).toInt,x(1).filter(!"A".contains(_)).toDouble,x(2),x(3),x(4),x(5),x(6).split("\\.")(2)))

val linesDF = splitLinesTuple.toDF("rank", "time", "name", "country", "dateb", "city", "dater")

linesDF.filter("name = 'Usain Bolt'")
    .groupBy("dater")
    .agg(Map("*" -> "count", "time" -> "avg"))
    .withColumnRenamed("COUNT(1)", "total_records")
    .withColumnRenamed("AVG(time)", "average_time")
    .select("dater", "total_records", "average_time")
    .orderBy(desc("total_records"), asc("average_time"))
    .limit(3)
    .show



val short = splitLines.filter(row => row.length < 7)
short.show

