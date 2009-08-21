package com.gstocco.TwitLucene

object SearchTwitter{
	import java.util.StringTokenizer
	import org.joda.time.DateTime

	import org.apache.lucene.analysis.standard.StandardAnalyzer
	import org.apache.lucene.analysis.{Analyzer,KeywordAnalyzer}
	import org.apache.lucene.document.Document
	import org.apache.lucene.index.IndexReader;
	import org.apache.lucene.index.Term;
	import org.apache.lucene.index.TermEnum;
	import org.apache.lucene.index.IndexReader
	import org.apache.lucene.queryParser.QueryParser
	import org.apache.lucene.search.{IndexSearcher,TopDocs,RangeFilter,Query,ConstantScoreRangeQuery,HitIterator,Hits,Hit,TopDocCollector}
	import org.apache.lucene.util.PriorityQueue


	import scala.actors._
	import scala.actors.Actor._
	import scala.collection.mutable.HashMap
	import scala.io.Source

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
				case "twitUserIds" =>
					val term = args(1)
					getTwitAndUserIdsForWord(term)
				case "userPairsForTerm" =>
					val term = args(1)
					getUserPairsForTerm(term)
				case "symPairsForTerm" =>
					val term = args(1)
					getSymPairsForTerm(term)
				case "topWordsByTwit" =>
					val twitFile=args(1)
					val stopFile = args(2)
					getTopWordsByTwit(twitFile,stopFile)
				case "topWordsByUser" =>
					val userFile=args(1)
					val stopFile = args(2)
					getTopWordsByUser(userFile,stopFile)
				case "userPairTopics" =>
					val userFile=args(1)
					val stopFile=args(2)
					val cutoff=args(3)
					getUserPairTopics(userFile,stopFile,Integer.parseInt(cutoff))
				case "getStopList" =>
					val cutoff=args(1)
					getStopList(Integer.parseInt(cutoff))
				case "help" =>
					printHelp
			}
		} catch{
			case e:Exception =>
				e.printStackTrace
				printHelp
		}
	}

	def printHelp{
		println("Run options: {hour term begin end} | {topChatters term numChatters} | {twitUserIds term} | {topWordsByTwit twitFile stopFile} | {topWordsByUser userFile stopFile} | {userPairTopics userFile stopFile cutoff} | {getStopList cutoff} | {help}")
	}

	def getSymPairsForTerm(term:String){
		val searcher:IndexSearcher = new IndexSearcher(IndexReader.open("/Users/gabe/Code/TwitTrends/theFullIndex"))
		val parser: QueryParser = new QueryParser("text",new KeywordAnalyzer())
		var query:Query = parser.parse(term)
		var hits = searcher.search(query)
		val hash = new HashMap[String, (int, String)]
		for(i <- 0 to hits.length-1){
			val docId = hits.id(i)
			val d:Document = searcher.doc(docId)
			val user = d.getField("user_id").stringValue
			val twitId = d.getField("id").stringValue
			if(hash.contains(user)){
				hash.update(user,( (hash.get(user).get._1) + 1 , (hash.get(user).get._2) +","+twitId ) )
			} 
			else{
				hash.put(user,(1,twitId))
			}
		}
		val alist = hash.toList
		val userMap = new HashMap[int, int]
		val origQuery = query.toString
		alist.foreach{(elem) => //foreach user
			query= parser.parse("+user_id:"+elem._1+" -in_reply_to_user_id:null "+origQuery)
			var hits = searcher.search(query)
			val uId = Integer.parseInt(elem._1)
			for(i <- 0 to hits.length-1){
				val docId = hits.id(i)
				val d:Document = searcher.doc(docId)
				val r_uId = Integer.parseInt(d.getField("in_reply_to_user_id").stringValue)
				if(hash.contains(d.getField("in_reply_to_user_id").stringValue)){
					if(!userMap.contains(Math.min(uId,r_uId))){
						userMap.put(Math.min(uId,r_uId),Math.max(uId,r_uId))
					}
				}
			}
		}
		val userList = userMap.toList
		userList.foreach{(elem) =>
			println(elem._1+","+elem._2)
		}
	}

	def getUserPairsForTerm(term:String){
		val searcher:IndexSearcher = new IndexSearcher(IndexReader.open("/Users/gabe/Code/TwitTrends/theFullIndex"))
		val parser: QueryParser = new QueryParser("text",new KeywordAnalyzer())
		var query:Query = parser.parse(term)
		var hits = searcher.search(query)
		val hash = new HashMap[String, (int, String)]
		for(i <- 0 to hits.length-1){
			val docId = hits.id(i)
			val d:Document = searcher.doc(docId)
			val user = d.getField("user_id").stringValue
			val twitId = d.getField("id").stringValue
			if(hash.contains(user)){
				hash.update(user,( (hash.get(user).get._1) + 1 , (hash.get(user).get._2) +","+twitId ) )
			} 
			else{
				hash.put(user,(1,twitId))
			}
		}
		val alist = hash.toList
		val userMap = new HashMap[int, int]
		alist.foreach{(elem) => //foreach user
			query= parser.parse("+user_id:"+elem._1+" -in_reply_to_user_id:null")
			var hits = searcher.search(query)
			val uId = Integer.parseInt(elem._1)
			for(i <- 0 to hits.length-1){
				val docId = hits.id(i)
				val d:Document = searcher.doc(docId)
				val r_uId = Integer.parseInt(d.getField("in_reply_to_user_id").stringValue)
				if(hash.contains(d.getField("in_reply_to_user_id").stringValue)){
					if(!userMap.contains(Math.min(uId,r_uId))){
						userMap.put(Math.min(uId,r_uId),Math.max(uId,r_uId))
					}
				}
			}
		}
		val userList = userMap.toList
		userList.foreach{(elem) =>
			println(elem._1+","+elem._2)
		}
	}

	def getTwitAndUserIdsForWord(term:String){
		val searcher:IndexSearcher = new IndexSearcher(IndexReader.open(index))
		val parser: QueryParser = new QueryParser("text",new TweetAnalyzer())
		val query:Query = parser.parse(term)
		var collector:TopDocCollector = new TopDocCollector(1)
		searcher.search(query,collector)
		collector = new TopDocCollector(collector.topDocs().totalHits)
		searcher.search(query,collector)
		val hits = collector.topDocs().scoreDocs
		val hash = new HashMap[String, (int, String)]
		for(i <- 0 to hits.length-1){
			val docId = hits(i).doc
			val d:Document = searcher.doc(docId)
			val user = d.getField("user_id").stringValue
			val twitId = d.getField("id").stringValue
			if(hash.contains(user)){
				hash.update(user,( (hash.get(user).get._1) + 1 , (hash.get(user).get._2) +","+twitId ) )
			} 
			else{
				hash.put(user,(1,twitId))
			}
		}
		val alist = hash.toList
		val sortedList:List[(String,(int,String))] = alist.sort((x,y) => x._2._1 > y._2._1)
		sortedList.foreach{(elem) =>
			println(elem._1+"\t"+elem._2._1+"\t"+elem._2._2)
		}
	}

	def getStopList(cutoff:Int){
		val reader:IndexReader = IndexReader.open(index)
		val terms:TermEnum = reader.terms
		val hash = new HashMap[String,int]
		var minFreq = 0
		while(terms.next){
			val field=terms.term.field
			if(field=="text"){
				if(terms.docFreq > minFreq){
					hash.put(terms.term.text,terms.docFreq)
				}
			}
		}
		val alist = hash.toList
		val sortedList = alist.sort((x,y) => x._2 > y._2)
		for(i<- 0 to cutoff){
			println(sortedList(i)._1)
		}
		reader.close
	}

	def getUserPairTopics(userFile:String,stopFile:String,cutoff:Int){
		val searcher:IndexSearcher = new IndexSearcher(IndexReader.open(index))
		val parser: QueryParser = new QueryParser("user_id",new KeywordAnalyzer())
		val hash = new HashMap[(String,String,String,String),List[(String,Int)]]
		var lines = Source.fromFile(userFile).getLines
		while(lines.hasNext){//foreach pair of users
			try{
				val line = lines.next.trim
				val users=line.split(",")
				var query:Query = parser.parse("user_id:"+users(0))
				var collector:TopDocCollector = new TopDocCollector(1)
				searcher.search(query,collector)
				var hits = collector.topDocs().scoreDocs
				val userName1=searcher.doc(hits(0).doc).getField("screen_name").stringValue
				query = parser.parse("user_id:"+users(1))
				collector = new TopDocCollector(1)
				searcher.search(query,collector)
				hits = collector.topDocs().scoreDocs
				val userName2=searcher.doc(hits(0).doc).getField("screen_name").stringValue
				query= parser.parse("(+user_id:"+users(0)+" +in_reply_to_user_id:"+users(1)+") OR (+user_id:"+users(1)+" +in_reply_to_user_id:"+users(0)+")")
				collector = new TopDocCollector(1)
				searcher.search(query,collector)
				collector = new TopDocCollector(collector.topDocs().totalHits)
				searcher.search(query,collector)
				hits = collector.topDocs().scoreDocs
				val pairHash = new HashMap[String, int]
				for(i<- 0 to hits.length-1){
					val docId = hits(i).doc
					val d:Document = searcher.doc(docId)
					val value = d.getField("text").stringValue
					val st = new StringTokenizer(value,"!.,;:'\"[]{}\\|+=_-)(*&^%$@!`~? ")
					while (st.hasMoreTokens()) {
						val temp = st.nextToken
						if(pairHash.contains(temp)){
							pairHash.update(temp,pairHash.get(temp).get+1)
						} 
						else{
							pairHash.put(temp,1)
						}
					}
				}
				val pairList = pairHash.toList
				val pairSortedList:List[(String,int)] = pairList.sort((x,y) => x._2 > y._2)
				if(Integer.parseInt(users(0))>Integer.parseInt(users(1))){
					hash.put((users(0),userName1,users(1),userName2),pairSortedList)
				}
				else{
					hash.put((users(1),userName2,users(0),userName1),pairSortedList)
				}
			}
			catch{
				case e:Exception =>
					e.printStackTrace
			}
		}
		var stopWordList = List[String]()
		var lines2 = Source.fromFile(stopFile).getLines
		while(lines2.hasNext){
			stopWordList ::= lines2.next.trim.toLowerCase
		}
		val tehList = hash.toList
		for (i <- 0 to tehList.length-1){
			print(tehList(i)._1._1+":"+tehList(i)._1._2+"\t"+tehList(i)._1._3+":"+tehList(i)._1._4+"\t")
			val list=tehList(i)._2
			for(j <- 0 to list.length-1){
				if(list(j)._2>=cutoff){
					if(!stopWordList.contains(list(j)._1.toLowerCase))
						print(list(j)._1.trim+" "+list(j)._2+",")
				}
			}
			print("\n")
		}
	}

	def getTopWordsByTwit(twitIdFile:String,stopWordFile:String){
		val searcher:IndexSearcher = new IndexSearcher(IndexReader.open(index))
		val parser: QueryParser = new QueryParser("id",new KeywordAnalyzer())
		val hash = new HashMap[String, int]
		var lines = Source.fromFile(twitIdFile).getLines
		while(lines.hasNext){
			try{
				val line = lines.next.trim
				val query:Query = parser.parse("id:"+line)
				val collector:TopDocCollector = new TopDocCollector(1)
				searcher.search(query,collector)
				val hits = collector.topDocs().scoreDocs
				val docId = hits(0).doc
				val d:Document = searcher.doc(docId)
				val value = d.getField("text").stringValue
				val st = new StringTokenizer(value,"!.,;:'\"[]{}\\|+=_-)(*&^%$#@!`~? ")
				while (st.hasMoreTokens()) {
					val temp = st.nextToken
					if(hash.contains(temp)){
						hash.update(temp,hash.get(temp).get+1)
					} 
					else{
						hash.put(temp,1)
					}
				}
			}
			catch{
				case e:Exception => e.printStackTrace
			}
		}
		var stopWordList = List[String]()
		lines = Source.fromFile(stopWordFile).getLines
		while(lines.hasNext){
			stopWordList ::= lines.next
		}
		val alist = hash.toList
		val sortedList:List[(String,int)] = alist.sort((x,y) => x._2 > y._2)
		for (i <- 0 to sortedList.length-1){
			if(!stopWordList.contains(sortedList(i)._1))
			println(sortedList(i)._1+","+sortedList(i)._2)
		}
	}

	def getTopWordsByUser(userIdFile:String,stopWordFile:String){
		val searcher:IndexSearcher = new IndexSearcher(IndexReader.open(index))
		val parser: QueryParser = new QueryParser("user_id",new KeywordAnalyzer())
		val hash = new HashMap[String, int]
		var lines = Source.fromFile(userIdFile).getLines
		while(lines.hasNext){
			val line = lines.next.trim
			val query:Query = parser.parse(line)
			var collector:TopDocCollector = new TopDocCollector(1)
			searcher.search(query,collector)
			collector = new TopDocCollector(collector.topDocs().totalHits)
			searcher.search(query,collector)
			val hits = collector.topDocs().scoreDocs
			for(i <- 0 to hits.length-1){
				try{
					val docId = hits(i).doc
					val d:Document = searcher.doc(docId)
					val value = d.getField("text").stringValue
					val st = new StringTokenizer(value,"!.,;:'\"[]{}\\|+=_-)(*&^%$#@!`~? ")
					while (st.hasMoreTokens()) {
						val temp = st.nextToken
						if(hash.contains(temp)){
							hash.update(temp,hash.get(temp).get+1)
						} 
						else{
							hash.put(temp,1)
						}
					}
				}
				catch{
					case e:Exception => e.printStackTrace
				}
			}
		}
		var stopWordList = List[String]()
		lines = Source.fromFile(stopWordFile).getLines
		while(lines.hasNext){
			stopWordList ::= lines.next
		}
		val alist = hash.toList
		val sortedList:List[(String,int)] = alist.sort((x,y) => x._2 > y._2)
		for (i <- 0 to sortedList.length-1){
			if(!stopWordList.contains(sortedList(i)._1))
			println(sortedList(i)._1+","+sortedList(i)._2)
		}
	}

	def getTopChatters(term:String,numChatters:Int){
		val searcher:IndexSearcher = new IndexSearcher(IndexReader.open(index))
		val parser: QueryParser = new QueryParser("location",new TweetAnalyzer())
		val query:Query = parser.parse(term)
		val collector:TopDocCollector = new TopDocCollector(20000000)
		searcher.search(query,collector)
		val hits = collector.topDocs().scoreDocs
		val hash = new HashMap[int, int]
		for(i <- 0 to hits.length-1){
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