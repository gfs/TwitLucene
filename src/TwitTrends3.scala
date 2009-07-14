import org.apache.lucene.analysis.Analyzer

class TweetAnalyzer extends Analyzer{
	import org.apache.lucene.analysis.{StopFilter,CharTokenizer,TokenStream}
	import java.io.Reader
	
	val STOP_WORDS = Array("0", "1", "2", "3", "4", "5", "6", "7", "8",
    "9", "000", "$",
    "a", "b", "c", "d", "e", "f", "g", "h", "i",
    "j", "k", "l", "m", "n", "o", "p", "q", "r",
    "s", "t", "u", "v", "w", "x", "y", "z")
	
	val STOP_SET = StopFilter.makeStopSet(STOP_WORDS)
	
	def tokenStream(fieldName:String, reader:Reader):TokenStream={
		return new StopFilter(new TweetTokenizer(reader),STOP_SET)
	}
	
	class TweetTokenizer(reader:Reader) extends CharTokenizer(reader:Reader){
		override def isTokenChar(c: char):boolean={
			c match{
				case '@' =>
					return true
				case '#' =>
					return true
				case x:char =>
					return Character.isLetter(x)
			}
		}
	}
}

object ParseJSON {
	import org.codehaus.jackson._
	import scala.io.Source
	import scala.actors._
	import scala.actors.Actor._
	import java.io.File
	import java.util.Date
	import org.apache.lucene.analysis.Analyzer
	import org.apache.lucene.document._
	import org.apache.lucene.index.{IndexWriter,Term,IndexReader}
	import org.apache.lucene.search.{Searcher,Query,TermQuery,IndexSearcher}
	import org.apache.lucene.queryParser.QueryParser;
	import org.joda.time.DateTime
	import org.joda.time.format.DateTimeFormat
	
	val ACTORS = 5
	val COMMIT_LEVEL = 10000
	val INDEX_DIR = new File("index");
	val dateTimeFmt = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY")
	
	def main(args: Array[String]){
		val date = new Date()
		val factory: JsonFactory = new JsonFactory();		
		val lines = Source.fromFile(args(0),"UTF-8").getLines

		val writer = new IndexWriter(args(0)+"_index", new TweetAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED)
		val reader = new ReadActor(date,lines,writer)
		reader.start
		for(i <- 1 to ACTORS){
			val pActor:ParseActor = new ParseActor(reader,writer,factory)
			pActor.start
		}
	}
	
	def RecurseJSON(doc:Document,input:List[_],pre:String){
		for (elem <- input){
			val (a,b) = elem
			b match{
				case x:List[_] =>
					RecurseJSON(doc,x,"user_")
				case null =>
				case _ =>
					a match{
						case "text" =>
							doc.add(new Field(pre+a, b.toString, Field.Store.YES, Field.Index.ANALYZED))
						case "created_at" =>
							val time: DateTime = try { dateTimeFmt.parseDateTime(b.toString)}
							catch { case _ => throw new Exception("Could not parse time.")}
							doc.add(new Field(pre+a, time.toString, Field.Store.YES, Field.Index.NOT_ANALYZED))
						case _ =>
							doc.add(new Field(pre+a, b.toString, Field.Store.NO, Field.Index.NO))
					}
			}
		}
	}
	
	class ReadActor(date:Date,lines:Iterator[_],writer:IndexWriter) extends Actor{
		def act(){
			var i=0
			var actorsRemain = ACTORS
			loop{
				react{
					case ("Gimme",actor:Actor) =>
						i=i+1
						if(i%COMMIT_LEVEL==0){
							print(".")
							writer.commit
						}
						if(lines.hasNext){
							var line:String = lines.next.toString
					 		if (!line.trim.isEmpty){
								actor ! line
								} else{
									actor ! "again"
								}
						} else{
							actor ! "Die"
							actorsRemain -= 1
							if (actorsRemain == 0){
								writer.close
								val date2 = new Date
								println("Time elapsed: "+(date2.getTime - date.getTime)+" ms")
							}
						}
							
				}
			}
		}
	}
	
	class ParseActor(parser:ReadActor,writer:IndexWriter,factory:JsonFactory) extends Actor{
		def act(){	
			loop{
				parser ! ("Gimme",this)
				var doc = new Document
				react{
					case "Die" =>
						this.exit
					case "again" =>	
						//Do nothing, allow it to ask for gimme again
					case x:String =>	
						try {
							val jp:JsonParser = factory.createJsonParser(x);
							jp.nextToken()
							while(jp.nextToken() != JsonToken.END_OBJECT){
								val fieldName = jp.getCurrentName
								jp.nextToken()
								fieldName match{
									case "user" =>
										while(jp.nextToken () != JsonToken.END_OBJECT){
											val nameField = jp.getCurrentName
											jp.nextToken()
										 	nameField match{
												case "screen_name" =>
													doc.add(new Field("screen_name", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
												case "location" =>
													doc.add(new Field("location", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
												case "id" =>
													doc.add(new Field("user_id", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
												case _ =>
											}
										}
									case "id" =>
										doc.add(new Field("id", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
									case "text" =>
										doc.add(new Field("text", jp.getText(), Field.Store.YES, Field.Index.ANALYZED))
									case "in_reply_to_user_id" =>
										doc.add(new Field("in_reply_to_user_id", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
									case "in_reply_to_screen_name" =>
										doc.add(new Field("in_reply_to_screen_name", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
									case "created_at" =>
										val time: DateTime = try { dateTimeFmt.parseDateTime(jp.getText())}
										catch { case _ => throw new Exception("Could not parse time.")}
										doc.add(new Field("created_at", time.toString, Field.Store.YES, Field.Index.NOT_ANALYZED))
									case "in_reply_to_status_id" =>
										doc.add(new Field("in_reply_to_status_id", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
									case _ =>
								}								
							}
							writer.addDocument(doc)
						} catch {
								case e: NullPointerException => 
								case e: Exception =>
									e.printStackTrace
						}
					case null =>
				}
			}
		}
	}
}

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