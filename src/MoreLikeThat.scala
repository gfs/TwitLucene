package com.gstocco.TwitLucene

import org.apache.lucene.index.IndexReader
import org.apache.lucene.document.Document
import org.apache.lucene.queryParser.QueryParser`
import org.apache.lucene.search.similar.MoreLikeThis
import org.apache.lucene.search.Hits
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query

object MoreLikeThat{
	def main(args:Array[String]){
		args(0) match{
			case ""
		}
	}
	
	def fromSet(inputSet:Set){
		val inList = inputSet.toList
		val ir = new IndexReader("theCorporaIndex")
		val is = new IndexSearcher(ir)
		val queryString = queryStringForList(inList,is)
		val mlt = new MoreLikeThis(ir)
		val arr = new Array[String](1)
		arr(0) = "text"
		mlt.setFieldNames(arr)
		mlt.setAnalyzer(new TweetAnalyzer)
		/*
		TODO: Set stopwords like this:
		mlt.setStopWords()
		*/
		val hits = is.search(mlt.like(new StringReader(queryString)))
	}
	def fromTwoSets(setA:Set,setB:Set){
		val listA=setA.toList
		val listB=setB.toList
		val ir = new IndexReader("theCorporaIndex")
		val is = new IndexSearcher(ir)
		var queryString=queryStringForList(listA,is)
		val qp = new QueryParser("user_id",new WhiteSpaceAnalyzer)
	}
	def queryStringForList(inList:List,is:IndexSearcher){
		var queryString = new String
		val qp = new QueryParser("user_id",new WhiteSpaceAnalyzer)
		inList.foreach{(elem) =>
			val hits = is.search(qp.parse(elem))
			val hit = hits.doc(0)
			queryString=queryString + " "+ hit.get("text")
		}
		return queryString
	}
}
