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
import org.apache.lucene.search.BooleanQuery

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
				val lines=Source.fromFile(args(1)).getLines
				var listA = List[String]()
				var listB = List[String]()
				var community = true
				while(lines.hasNext){
					val line = lines.next.trim
					if(line.substring(0,1)!="#"){
						val tokens = line.split(",")
						if(community){
							tokens.foreach{(elem)=>
								listA += elem	
							}
							community = false
						}
						else{
							tokens.foreach{(elem)=>
								listB += elem	
							}
							community=true
							fromTwoLists(listA,listB)
							listA=List[String]()
							listB=List[String]()
						}
					}
				}
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
		BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
		val ir = IndexReader.open("theCorporaIndex")
		val is = new IndexSearcher(ir)
		var query=queryStringForList(listA,is)
		val qp = new QueryParser("text",new TweetAnalyzer)
		var str = new String()
		if(listB.length>=1){
			str+="("
			listB.foreach{(elem)=>
				str += "user_id:"+elem+" "
			}
			str += ") AND ("
		}		
		str += query+")"
		var nQuery = qp.parse(str)
		var fixdQuery = nQuery.toString
		var i=0
		while(fixdQuery.indexOf('-')!=(-1)){
			fixdQuery = fixdQuery.substring(0,fixdQuery.indexOf('-'))+fixdQuery.substring(fixdQuery.indexOf('-')+1)
			nQuery = qp.parse(fixdQuery)
			fixdQuery = nQuery.toString
			i+=1
		}
		while(fixdQuery.indexOf('+')!=(-1)){
			fixdQuery = fixdQuery.substring(0,fixdQuery.indexOf('+'))+fixdQuery.substring(fixdQuery.indexOf('+')+1)
			nQuery = qp.parse(fixdQuery)
			fixdQuery = nQuery.toString
			i+=1
		}
		// println(nQuery)
		val collector:TopDocCollector = new TopDocCollector(listB.length)
		is.search(nQuery,collector)
		val hits = collector.topDocs().scoreDocs
		for(i <- 0 to hits.length-1){
			val docId = hits(i).doc
			val d:Document = is.doc(docId)
			val user = d.getField("user_id").stringValue
			val score = hits(i).score
			println(score +"\t"+ user)
		}
		println("\n----------------------------------------------------------\n")
		Thread.sleep(10000)
	}
	
	def queryStringForList(inList:List[String],is:IndexSearcher):String={
		var queryString = new String
		val qp = new QueryParser("user_id",new WhitespaceAnalyzer)
		inList.foreach{(elem) =>
			try{
				val hits = is.search(qp.parse(elem))
				val hit = hits.doc(0)
				// var tokens = hit.get("text").split(" ")
				// tokens.foreach{(elem)=>
				// 	queryString += elem.trim
				// }
				// queryString=queryString + " "+ hit.get("text").trim
				hit.get("text").foreach{(char)=>
					char match{
						case '#' =>
							queryString += char
						case '@' =>
							queryString += char
						case ' ' =>
							queryString += char
						case char:char=>
							if(('a' to 'z').contains(char) || ('A' to 'Z').contains(char) || ('0' to '9').contains(char)){
								queryString += char
							}
					}
				}
			}
			catch{
				case e:Exception => e.printStackTrace
				println("No results for "+elem+"?")
			}
		}
		return queryString
	}
	
}
