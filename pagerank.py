from pyspark import SparkContext, SparkConf

master = "spark://L3701100067:7077"
conf = SparkConf()\
    .setAppName("PageRank")\
    .setMaster(master)\
    .set("spark.cores.max", 6)\
    .set("spark.executor.memory", '4g')
sparkContext = SparkContext(conf=conf)

lines = sparkContext.textFile("file:///Users/p3700698/small.txt")
pageWithLinks = lines.map(lambda line: (line.split(',')[0], line.split(',')[1:]))

ranks = pageWithLinks.map(lambda (pageId, links) : (pageId, 1.0))
for i in range(0,3):
    contributions = pageWithLinks.join(ranks).flatMap(
        lambda (pageId, (links, pageRank)) : map(lambda link: (link, pageRank / len(links)), links)
    )
    ranks = contributions.reduceByKey(lambda contributionX, contributionY : contributionX + contributionY).map(lambda (link, contribution) : (link, 0.15 + 0.85 * contribution))
ranks.saveAsTextFile("file:///Users/p3700698/result.txt")



