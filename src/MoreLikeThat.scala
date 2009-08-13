package com.gstocco.TwitLucene

import java.io.StringReader

import java.util.TreeSet

import org.apache.lucene.analysis.WhitespaceAnalyzer
import org.apache.lucene.index.IndexReader
import org.apache.lucene.document.Document
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.search.similar.MoreLikeThis
import org.apache.lucene.search.Hits
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import org.apache.lucene.search.TopDocCollector

import scala.io.Source

object MoreLikeThat{
	def main(args:Array[String]){
		args(0) match{
			case "oneFile"=>
				var lines = Source.fromFile(args(1)).getLines
				var list = List[String]()
				while(lines.hasNext){
					val line = lines.next.trim
					list += line
				}
				lines = Source.fromFile(args(2)).getLines
				var stopSet:TreeSet[String] = new TreeSet[String]()
				while(lines.hasNext){
					val line = lines.next.trim
					stopSet.add(line)
				}
				fromList(list,stopSet.asInstanceOf[java.util.Set[String]])
			case "twoFiles"=>
				var lines = Source.fromFile(args(1)).getLines
				var listA = List[String]()
				while(lines.hasNext){
					val line = lines.next.trim
					listA += line
				}
				lines = Source.fromFile(args(1)).getLines
				var listB = List[String]()
				while(lines.hasNext){
					val line = lines.next.trim
					listB += line
				}
				fromTwoLists(listA,listB)
			case "alexyFormat"=>
				var lines=Source.fromFile(args(1)).getLines
				var listA = List[String]()
				var listB = List[String]()
				while(lines.hasNext){
					var line = lines.next.trim
					if(line.substring(0,1)!="#"){
						var tokens = line.split(",")
						tokens[0].substring(0,1) match{
							case "c"=>
								tokens.foreach{(elem)=>
									listA += elem.substring(1,elem.length-1)
								}
							case "f"=>
								tokens.foreach{(elem)=>
									listB += elem.substring(1,elem.length-1)
								}
						}
					}
				}
				fromTwoLists(listA,listB)
		}
	}
	
	
	def fromList(inList:List[String],stopSet:java.util.	Set[String]){
			val ir = IndexReader.open("theCorporaIndex")
			val is = new IndexSearcher(ir)
			val queryString = queryStringForList(inList,is)
			val mlt = new MoreLikeThis(ir)
			val arr = new Array[String](1)
			arr(0) = "text"
			mlt.setFieldNames(arr)
			mlt.setAnalyzer(new TweetAnalyzer)
			mlt.setStopWords(stopSet)
			val query = mlt.like(new StringReader(queryString))
			var collector:TopDocCollector = new TopDocCollector(1)
			is.search(query,collector)
			collector = new TopDocCollector(collector.topDocs().totalHits)
			is.search(query,collector)
			val hits = collector.topDocs().scoreDocs
			for(i <- 0 to hits.length-1){
				val docId = hits(i).doc
				val d:Document = is.doc(docId)
				val user = d.getField("user_id").stringValue
				val text = d.getField("text").stringValue
				if(hits(i).score>1)
					println(hits(i).score+"\t"+user+"\t"+text)
			}
	}
	
	// def fromSet(inputSet:Set){
	// 	val inList = inputSet.toList
	// 	fromList(inList)
	// }
	//
	// def fromTwoSets(setA:Set,setB:Set){
	// 	val listA=setA.toList
	// 	val listB=setB.toList
	// 	val ir = new IndexReader("theCorporaIndex")
	// 	val is = new IndexSearcher(ir)
	// 	var queryString=queryStringForList(listA,is)
	// 	val qp = new QueryParser("user_id",new WhiteSpaceAnalyzer)
	// }
		
	def fromTwoLists(listA:List[String],listB:List[String]){
		val ir = IndexReader.open("theCorporaIndex")
		val is = new IndexSearcher(ir)
		var queryString=queryStringForList(listA,is)
		val mlt = new MoreLikeThis(ir)
		val arr = new Array[String](1)
		arr(0) = "text"
		mlt.setFieldNames(arr)
		mlt.setAnalyzer(new TweetAnalyzer)
		val query = mlt.like(new StringReader(queryString))
		val qp = new QueryParser("user_id",new TweetAnalyzer)
		var str = new String("(")
		listB.foreach{(elem)=>
			str += "user_id:"+elem+" "
		}
		str += ") AND "
		str += query.toString
		val nQuery = qp.parse(str)
		var collector:TopDocCollector = new TopDocCollector(1)
		is.search(nQuery,collector)
		collector = new TopDocCollector(collector.topDocs().totalHits)
		is.search(nQuery,collector)
		val hits = collector.topDocs().scoreDocs
		for(i <- 0 to hits.length-1){
			val docId = hits(i).doc
			val d:Document = is.doc(docId)
			val user = d.getField("user_id").stringValue
			println(user)
		}
	}
	
	def queryStringForList(inList:List[String],is:IndexSearcher):String={
		var queryString = new String
		val qp = new QueryParser("user_id",new WhitespaceAnalyzer)
		inList.foreach{(elem) =>
			try{
				val hits = is.search(qp.parse(elem))
				val hit = hits.doc(0)
				queryString=queryString + " "+ hit.get("text")
			}
			catch{
				case _ => println("No results for "+elem)
			}
		}
		return queryString
	}
	
}
