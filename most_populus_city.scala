 val df = spark.
      read.
      option("header", "true").
      csv("/home/raju/jacek/popcites.csv")
  
    var regex = "\\s+"
    var res1 = df.withColumn("clean_popu" , regexp_replace(col("population"), regex, "").cast("integer"))

    var res2 = res1.select("country", "clean_popu").groupBy("country").agg(max($"clean_popu") as "high")
 
    res1.show()
    res2.show()

    var joinexpression1 = res1.col("country") ===  res2.col("country")
    var joinexpression2 = res1.col("clean_popu") === res2.col("high")

    var finalx = res1.join(res2, joinexpression1 && joinexpression2, "inner").
      drop(res1.col("country")).drop(res1.col("population")).drop(res1.col("clean_popu"))

    finalx.show()