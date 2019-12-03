import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import org.checkerframework.checker.units.qual.s

import scala.xml.XML



class MyMapperReducerTests extends FunSuite {
  val conf = ConfigFactory.load()

  test("Able to Load Start Tags and End Tags") {

    val startTags = conf.getString("START_TAGS")
    val endTags = conf.getString("END_TAGS")
    assert(!startTags.isEmpty)
    assert(!endTags.isEmpty)
  }

  test("The Output Folders are configured with configurations") {

    val PATH_OUTPUT1 = conf.getString("PATH_OUTPUT1")
    val PATH_OUTPUT2 = conf.getString("PATH_OUTPUT2")
    val PATH_OUTPUT3 = conf.getString("PATH_OUTPUT3")
    val PATH_OUTPUT4 = conf.getString("PATH_OUTPUT4")
    val PATH_OUTPUT5 = conf.getString("PATH_OUTPUT5")

    assert(!PATH_OUTPUT2.isEmpty)
    assert(!PATH_OUTPUT1.isEmpty)
    assert(!PATH_OUTPUT3.isEmpty)
    assert(!PATH_OUTPUT4.isEmpty)
    assert(!PATH_OUTPUT5.isEmpty)
  }

  test("Test for Checking Median") {
    // using Logic from MyStatReducer
    var myList: List[Int] = List(5, 4, 1, 2, 3)
    myList = myList.sorted // creating the sorted list that will help in getting the median
    var median: Double = 0.0
    val myListLen = myList.length
    if (myListLen == 1) {
      median = myList(0)
    } else if (myListLen % 2 == 0) {
      median = (myList(myListLen / 2 - 1) + myList(myListLen / 2)) / 2.0
    } else if (myListLen % 2 != 0) {
      median = myList(myListLen / 2)
    }
    assert(median == 3)
  }


  test("Test for Mean for Number of Collaborations") {
    var myList: List[Int] = List(5, 4, 1, 2, 3)
    myList = myList.sorted // creating the sorted list that will help in getting the median
    val mean: Double = myList.sum / myList.length
    assert(mean == 3)
  }

  test("Test for MaxCollaboration ") {
    var myList: List[Int] = List(5, 4, 1, 2, 3)
    myList = myList.sorted // creating the sorted list that will help in getting the median
    val maxCollaborators = myList.max
    assert(maxCollaborators == 5)
  }


  test("Author count within the xml chunk for a particular publication ") {
    val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI

    val value1 =
      s"""<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13">
      <author>Mark Grechanik</author>
      <author>B. M. Mainul Hossain</author>
      <author>Ugo Buy</author>
      <title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>
      <pages>174-183</pages>
      <year>2013</year>
      <booktitle>ICST</booktitle>
      <ee>https://doi.org/10.1109/ICST.2013.19</ee>
      <ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>
      <crossref>conf/icst/2013</crossref>
      <url>db/conf/icst/icst2013.html#GrechanikHB13</url></inproceedings>"""


    val xmlToProcess =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
    <!DOCTYPE dblp SYSTEM "$dtdFilePath">
    <dblp>""" + value1 + "</dblp>" // adding root to the extracted xml chunk

    val proceedings = XML.loadString(xmlToProcess)

    val authors = (proceedings \\ "author").map(author => author.text.toLowerCase.trim).toList.sorted

    assert(authors.length == 3)
  }


  test("Authorship Score for authors for a publication") {
    val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val value =
      s"""<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13">
      <author>Mark Grechanik</author>
      <author>B. M. Mainul Hossain</author>
      <title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>
      <pages>174-183</pages>
      <year>2013</year>
      <booktitle>ICST</booktitle>
      <ee>https://doi.org/10.1109/ICST.2013.19</ee>
      <ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>
      <crossref>conf/icst/2013</crossref>
      <url>db/conf/icst/icst2013.html#GrechanikHB13</url>
    </inproceedings>"""
    val xmlToProcess =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
    <!DOCTYPE dblp SYSTEM "$dtdFilePath">
    <dblp>""" + value + "</dblp>" // adding root to the extracted xml chunk

    val proceedings = XML.loadString(xmlToProcess)

    val authors = (proceedings \\ "author").map(author => author.text.toLowerCase.trim).toList
    var mapMut: Map[String, Double] = Map("Demo" -> 0.0)

    val factor: Double = 1.0 / authors.size
    var carryOver: Double = 0.0
    var authorshipScore: Double = 0.0

    for (eachAuthor <- authors.reverse) {
      if (eachAuthor != authors.head) {
        authorshipScore = (factor + carryOver) * 3.0 / 4.0
        carryOver = (factor + carryOver) * 1.0 / 4.0
      }
      else {
        authorshipScore = factor + carryOver
      }
      mapMut += (eachAuthor -> authorshipScore)
    }
    assert(mapMut("mark grechanik") == 0.625)
    assert(mapMut("b. m. mainul hossain") == 0.375)
  }
}

