package com.Societe_Generale_test.scala

import org.apache.spark.sql.SparkSession

object Problem_1 {
  def final_report() {
    val spark = SparkSession.builder().master("local")
      .appName("Poverty Analysis")
      .getOrCreate()

    val StatesName_df =spark.read.option("header",true).option("inferSchema","true")
      .csv("C:\\Users\\akijh\\Downloads\\problem1\\StatesName.csv")

    val PovertyEstimates_df =spark.read.option("header",true).option("inferSchema","true")
      .csv("C:\\Users\\akijh\\Downloads\\problem1\\PovertyEstimates.csv")

    var joinedData= PovertyEstimates_df.join(StatesName_df, PovertyEstimates_df("Stabr")===
      StatesName_df("Postal Abbreviation"),"inner")

     joinedData= joinedData.withColumnRenamed("Capital Name","State")

    var df1= joinedData.select("State","Area_name","Stabr","Urban_Influence_Code_2003",
      "Rural-urban_Continuum_Code_2013","PCTPOV017_2018")

    df1=df1.withColumnRenamed("Rural-urban_Continuum_Code_2013","Rural_urban_Continuum_Code_2013")


    df1.createOrReplaceTempView("df1")

    var fnl_rpt= spark.sql("select State, CONCAT(Area_name, ' ', Stabr) as Area_Name,Urban_Influence_Code_2003" +
      ",Rural_urban_Continuum_Code_2013, (100-PCTPOV017_2018) as POV_elder_than17_2018 from df1 " +
      " where  Urban_Influence_Code_2003 % 2 != 0 and Rural_urban_Continuum_Code_2013 % 2 =0" )

    fnl_rpt.show()



  }
}
