package com.stocco.TwitLucene

object SearchTwitter{
	
	import org.joda.time.DateTime
	import org.apache.lucene.search.{IndexSearcher,TopDocs,RangeFilter,Query,ConstantScoreRangeQuery,HitIterator,Hits,Hit,TopDocCollector}
	import org.apache.lucene.document.Document
	import org.apache.lucene.queryParser.QueryParser
	import org.apache.lucene.index.IndexReader
	import org.apache.lucene.analysis.standard.StandardAnalyzer
	import org.apache.lucene.analysis.Analyzer
	import org.apache.lucene.analysis.KeywordAnalyzer
	import scala.actors._
	import scala.actors.Actor._
	import scala.collection.mutable.HashMap
	import org.apache.lucene.search.Hits
	
	val index = "theFullIndex"
	
	def main(args: Array[String]){
		try {
			val mode = args(0)
			val start = new DateTime
			mode match{
				case "hour" =>
					val term = args(1)
					val beg = args(2)
					val end = args(3)
					getHourResults((2009 to 2009), (06 to 06), (beg.toInt to end.toInt), (0 to 23), term)
				case "topChatters" =>
					val term = args(1)
					val numChatters = args(2)
					getTopChatters(term,numChatters.toInt)
				case "help" =>
					printHelp
			}
		} catch{
			case _ =>
				printHelp
		}
	}
	
	def printHelp{
		println("Run options: {hour term begin end} | {topChatters term numChatters} | {help}")
	}

	def getTopChatters(term:String,numChatters:Int){
		import scala.collection.mutable.HashMap
		
		val searcher:IndexSearcher = new IndexSearcher(IndexReader.open(index))
		val parser: QueryParser = new QueryParser("location",new TweetAnalyzer())
		val query:Query = parser.parse(term)
		val collector:TopDocCollector = new TopDocCollector(20000000)
		searcher.search(query,collector)
		val hits = collector.topDocs().scoreDocs
		val hash = new HashMap[int, int]
		for(i <- 1 to hits.length-1){
			val docId = hits(i).doc
			val d:Document = searcher.doc(docId)
			val value = d.getField("user_id").stringValue
			if(hash.contains(value.toInt)){
				val num:Int = hash.get(value.toInt).get
				hash.update(value.toInt,num+1)
			} else{
				hash.put(value.toInt,1)
			}
		}
		val alist = hash.toList
		val sortedList:List[(int,int)] = alist.sort((x,y) => x._2 > y._2)//Sort by number of reply messages each user has posted
		print("follow=")
		for (i <- 1 to numChatters){
			print(sortedList(i)._1)
			if(i<numChatters)
				print(",")
			else
				print("\n")
		}
	}

	def getHourResults(year:Range,month:Range,day:Range,hour:Range,term:String){//Different functions for different granulatrities?
		val searcher = new IndexSearcher(IndexReader.open(index));
		for (curYear <- year){
			for (curMon <- month){
				for (curDay <- day){
					// val index = genIndexString(curYear,curMon,curDay)
					for (curHour <- hour){
						val sActor = new SearchActor(term,genDateStrings(curYear,curMon,curDay,curHour),index,searcher)
						sActor.start()
					}
				}
			}
		}
	}
	
	def genIndexString(year:Int, mon:Int, day:Int):(String)={
		var indMon=mon.toString
		var indDay=day.toString
		if (mon < 10)
			indMon = "0" + mon
		if (day < 10)
			indDay = "0" + day
		return "tweets."+year+"-"+indMon+"-"+indDay+"_index"
	}

	//Fixes leading zeroes on dates
	def genDateStrings(year: Int, mon: Int, day: Int):(String,String)={
		val start = new DateTime(year,mon,day,0,0,0,0)
		val end = start.plusDays(1)
		return (start.toString(),end.toString())
	}
	
	def genDateStrings(year: Int, mon: Int, day: Int, hour: Int):(String,String)={
		val start = new DateTime(year,mon,day,hour,0,0,0)
		val end = start.plusHours(1)
		return (start.toString(),end.toString())
	}
	
	class SearchActor(term:String,dates:(String,String),index:String,searcher:IndexSearcher) extends Actor{
		def act(){
			val (start,end) = dates
			val parser: QueryParser = new QueryParser("location",new TweetAnalyzer())
			val query:Query = parser.parse(term)
			val hits:TopDocs = searcher.search(query,new RangeFilter("created_at",start,end,true,false),1)
			val query2: ConstantScoreRangeQuery = new ConstantScoreRangeQuery("created_at",start,end,true,false)
			val totals:Hits = searcher.search(query2)
			println(start+"\t"+hits.totalHits+"\t"+totals.length)
		}
	}
}