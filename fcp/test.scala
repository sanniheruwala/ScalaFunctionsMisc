package com.rxcorp.bdf.datalake.de9.transformations

import scala.io.Source

/**
  * Created by SHeruwala on 9/5/2018.
  */
object test extends App {

  /**
    * Read Files
    */

  val cust: Seq[(String, String)] = Source
    .fromFile("C:\\Users\\SHeruwala\\IQVIA\\Germany\\com.rxcorp.bdf.datalake.de9.ch2\\datalake-de9-ch2-scala\\src\\main\\scala\\cust_geo3.csv")
    .getLines
    .drop(1)
    .toList
    .map(x => (x.split(",", 0)(0).toString, x.split(",", 0)(1).toString))

  val std: Seq[(String, String)] = Source
    .fromFile("C:\\Users\\SHeruwala\\IQVIA\\Germany\\com.rxcorp.bdf.datalake.de9.ch2\\datalake-de9-ch2-scala\\src\\main\\scala\\std_geo3.csv")
    .getLines
    .drop(1)
    .toList
    .map(x => (x.split(",", 0)(0).toString, x.split(",", 0)(1).toString))

  /**
    * Joining based on landscape code.
    */

  var join: Seq[(String, (String, String))] = (cust ++ std)
    .groupBy(_._1)
    .map { case (key, tuples) => (key, tuples.map(_._2)) }
    .toList
    .map { x => (x._1, (x._2(0), x._2(1))) }

  /**
    * Selecting distinct Pairs.
    */
  val pairs = join.map { case (key, value) => value }.distinct

  /**
    * Creating Key-pair values
    * by grouping custom_geo_level(pair1)
    * and standard_geo_level(pair2).
    */
  var pair1 = join.groupBy(_._2._1)
  var pair2 = join.groupBy(_._2._2)

  var matchedList = List[(String, String, String, Int, String)]()
  var counter = 0
  var land, custL, stndL, typ = ""

  /**
    * Those which are having direct
    * mapping are given incremental values
    * and assigned them as integral.
    */
  pairs.foreach { x =>
    val intr = pair1.get(x._1).get.intersect(pair2.get(x._2).get)
    if (intr.length == pair1.get(x._1).get.length && intr.length == pair2.get(x._2).get.length) {
      counter = counter + 1
      pair1.get(x._1).get.foreach { y =>
        land = y._1
        custL = y._2._1
        stndL = y._2._2
        typ = "I"
        matchedList ::= (land, custL, stndL, counter, typ)
        pair1 = pair1.filterKeys(_ != x._1)
        pair2 = pair2.filterKeys(_ != x._2)
      }
    }
  }

  /**
    * Clubing the sequential
    * un-matched data.
    */
  var dm2 = List[List[(String, (String, String))]]()

  pair1.foreach { x =>
    var dm1 = List[(String, (String, String))]()
    x._2.foreach { y =>
      dm1 = dm1 ++: ((x._2.toList ++: pair2.get(y._2._2).get.toList))
    }
    dm2 ::= dm1.distinct.sorted
  }

  /**
    * Merging partial matched data.
    */
  var dm3 = List[List[(String, (String, String))]]()
  var dm4 = List[List[(String, (String, String))]]()

  dm2.distinct.foreach { x =>
    dm2.distinct.filter(_ != x).foreach { y =>
      if (x.intersect(y).length > 0) {
        dm3 ::= (x ::: y).sorted.distinct
      }
    }
    dm2.distinct.filter(_ != x).foreach { y =>
      if (x.intersect(y).length != 0) {
        dm4 ::= x.sorted
      }
    }
  }

  val fractional = dm2.distinct.filter(d => !dm4.exists(_ == d)) ::: dm3.distinct

  /**
    * Inserting to matchedList with incremental counter.
    */

  var map = matchedList.map(z => (z._1, z)).toMap

  var status = false
  var tmpC = 0
  fractional.foreach { x =>
    map = matchedList.map(z => (z._1, z)).toMap

    x.foreach { y =>
      if (map.contains(y._1)) {
        status = true
        tmpC = map.get(y._1).get._4
      }
    }

    if (status == true) {
      x.foreach { y =>
        land = y._1
        custL = y._2._1
        stndL = y._2._2
        typ = "F"
        matchedList ::= (land, custL, stndL, tmpC, typ)
      }
    }
    if (status == false) {
      counter = counter + 1
      x.foreach { y =>
        land = y._1
        custL = y._2._1
        stndL = y._2._2
        typ = "F"
        matchedList ::= (land, custL, stndL, counter, typ)
      }
    }

    status = false
  }
  println(matchedList.distinct.sortBy(_._4))
}
