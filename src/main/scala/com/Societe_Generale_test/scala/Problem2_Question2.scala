package com.Societe_Generale_test.scala

import org.apache.spark.sql.SparkSession

object Problem2_Question2 {
  def ServiceAgency2() {
    val spark = SparkSession.builder().master("local")
      .appName("Poverty Analysis")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("C:\\Users\\akijh\\Downloads\\problem2\\auth.csv")

    val header = rdd.first()

    val rdd_Without_header = rdd.filter(_ != header)

    var auth_cust = rdd_Without_header.map(line => {
      val colarray = line.split(",")
      List(colarray(2), colarray(3), colarray(128))
    })

    auth_cust.take(5).foreach(println(_))

    def filterCriteria(line : List[String]) ={
      var returnRecord = ""
      if(line(1).forall(_.isDigit)==true && !(line(2).equals("Delhi")) && line(0) > "650000"){
        returnRecord = line(1).toString()
      }

      returnRecord.toString()
    }

    val rdd2 = auth_cust.map(x=>filterCriteria(x))
    val ServiceAgency= rdd2.filter(x => x.trim().length > 0).collect()

    ServiceAgency.foreach(println(_))

  }
}
