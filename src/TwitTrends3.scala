import org.joda.time.DateTime
import org.apache.lucene.search.{IndexSearcher,TopDocs,RangeFilter,Query}
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.index.IndexReader
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.KeywordAnalyzer
import scala.actors._
import scala.actors.Actor._
import scala.collection.mutable.HashMap
import org.apache.lucene.search.Hits

//Have to allow arbitrary granularity as well as arbitrary ranges
//To keep queries fast must use 1 hour granularity or smaller at all times

object SearchTwitter{
	val beg = 11
	val end = 20
	val term = "tehran*"
	
	def main(args: Array[String]){
		val start = new DateTime
		getHourResults((2009 to 2009), (06 to 06), (11 to 22), (0 to 23), term)
	}

	def getHourResults(year:Range,month:Range,day:Range,hour:Range,term:String){//Different functions for different granulatrities?
		for (curYear <- year){
			for (curMon <- month){
				for (curDay <- day){
					val index = genIndexString(curYear,curMon,curDay)
					val searcher = new IndexSearcher(IndexReader.open(index));
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
			val parser2: QueryParser = new QueryParser("created_at",new KeywordAnalyzer())
			val query2:Query = parser.parse("["+start+" TO "+end+"]")
			val totals:TopDocs = searcher.search(query, null, 1)
			println(start+"\t"+hits.totalHits+"\t"+totals.totalHits+"\t"+"["+start+" TO "+end+"]")
		}
	}
	
	class TweetAnalyzer extends Analyzer{
		import org.apache.lucene.analysis.StopFilter
		import org.apache.lucene.analysis.CharTokenizer
		import org.apache.lucene.analysis.TokenStream
		import java.io.Reader
		
		val STOP_WORDS = Array("0", "1", "2", "3", "4", "5", "6", "7", "8",
        "9", "000", "$",
        // "about", "after", "all", "also", "an", "and",
        // "another", "any", "are", "as", "at", "be",
        // "because", "been", "before", "being", "between",
        // "both", "but", "by", "came", "can", "come",
        // "could", "did", "do", "does", "each", "else",
        // "for", "from", "get", "got", "has", "had",
        // "he", "have", "her", "here", "him", "himself",
        // "his", "how","if", "in", "into", "is", "it",
        // "its", "just", "like", "make", "many", "me",
        // "might", "more", "most", "much", "must", "my",
        // "never", "now", "of", "on", "only", "or",
        // "other", "our", "out", "over", "re", "said",
        // "same", "see", "should", "since", "so", "some",
        // "still", "such", "take", "than", "that", "the",
        // "their", "them", "then", "there", "these",
        // "they", "this", "those", "through", "to", "too",
        // "under", "up", "use", "very", "want", "was",
        // "way", "we", "well", "were", "what", "when",
        // "where", "which", "while", "who", "will",
        // "with", "would", "you", "your",
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
}