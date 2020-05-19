package com.Societe_Generale_test.scala

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Problem2_Question1 {
  def filterCriteria(line : List[String]) ={
    var returnRecord = ""
    if(line(3).forall(_.isDigit)==true && !(line(128).equals("Delhi")) && line(2) > "650000"){
      returnRecord = line(3).toString()
    }

    returnRecord.toString()
  }

  def ServiceAgency(){

    val bufferedSource = Source.fromFile("C:\\Users\\akijh\\Downloads\\problem2\\auth.csv")
    var serviceAgency = new ListBuffer[String]()
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
      val returnRecord = filterCriteria(cols.toList)
      if(!(returnRecord.isEmpty())){
        serviceAgency+=returnRecord
      }
    }
    bufferedSource.close
    //println(serviceAgency)
    serviceAgency.foreach((element:String) => println(element+" "))
  }
}
