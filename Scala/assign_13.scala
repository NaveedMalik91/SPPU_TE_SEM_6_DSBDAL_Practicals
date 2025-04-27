val linesRDD = sc.textFile("file:///home/naveed-malik/input.txt")
val wordsRDD = linesRDD.flatMap(_.split(" "))
val wordsKvRDD = wordsRDD.map((_,1))
val wordscount = wordsKvRDD.reduceByKey(_+_)
wordscount.collect().foreach(println)

