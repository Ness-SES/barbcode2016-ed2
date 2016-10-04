from pyspark import SparkContext, SparkConf, StorageLevel

master = "spark://L3701100067:7077"
conf = SparkConf()\
    .setAppName("PageRank-Optimized")\
    .setMaster(master)\
    .set("spark.cores.max", 6)\
    .set("spark.executor.memory", '4g')
sparkContext = SparkContext(conf=conf)

lines = sparkContext.textFile("file:///Users/p3700698/small.txt")
pageWithLinks = lines.map(lambda line: (line.split(',')[0], line.split(',')[1:])).partitionBy(60).persist(StorageLevel.MEMORY_AND_DISK_SER)

ranks = pageWithLinks.mapValues(lambda links : 1.0)
for i in range(0,3):
    contributions = pageWithLinks.join(ranks).flatMap(
        lambda (pageId, (links, pageRank)) : map(lambda link: (link, pageRank / len(links)), links)
    )
    ranks = contributions.reduceByKey(lambda contributionX, contributionY : contributionX + contributionY).mapValues(lambda contribution : 0.15 + 0.85 * contribution)
ranks.saveAsTextFile("file:///Users/p3700698/result-optimized.txt")
