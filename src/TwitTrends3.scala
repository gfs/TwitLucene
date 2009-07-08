import org.joda.time.DateTime
import org.apache.lucene.search.{IndexSearcher,TopDocs,RangeFilter}
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.index.IndexReader
import org.apache.lucene.analysis.standard.StandardAnalyzer
import scala.actors._
import scala.actors.Actor._
import scala.collection.mutable.HashMap

//Have to allow arbitrary granularity as well as arbitrary ranges
//To keep queries fast must use 1 hour granularity or smaller at all times

object SearchTwitter{
	val beg = 11
	val end = 20
	val term = "tehran*"
	val location = "tehran"
	
	def main(args: Array[String]){
	//	getDayResults((2009 to 2009), (06 to 06), (11 to 11), term)
		val start = new DateTime
		getHourResults((2009 to 2009), (06 to 06), (11 to 14), (0 to 23), term)
	}
	
	// def getDayResults(year:Range,month:Range,day:Range,term:String){//Different functions for different granulatrities?
	// 	val waitActor = new WaitActor()
	// 	for (curYear <- year){
	// 		for (curMon <- month){
	// 			for (curDay <- day){
	// 				val (start, end) = genDateStrings(curYear,curMon,curDay)
	// 				val sActor = new SearchActor(term,start,end,index,waitActor)
	// 				sActor.start()
	// 			}
	// 		}
	// 	}
	// }
	
	def getHourResults(year:Range,month:Range,day:Range,hour:Range,term:String){//Different functions for different granulatrities?
		val waitActor = new WaitActor()
		for (curYear <- year){
			for (curMon <- month){
				for (curDay <- day){
					// for (indexDay <- curDay-1 to curDay+1){

						val index = genIndexString(curYear,curMon,curDay)
						for (curHour <- hour){
							val (start, end) = genDateStrings(curYear,curMon,curDay,curHour)
							val sActor = new SearchActor(term,start,end,index,waitActor)
							sActor.start()
						}
					// }
				}
			}
		}
		waitActor.start()
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
	
	class WaitActor extends Actor{
		var toWait = 0
		val hash = new HashMap[String,Int]
		def act(){
			loop{
				react{
					case x:(String,Int) =>
					//	val (key,elem)=x
						toWait = toWait - 1
						if(toWait == 0){
					//		hash.foreach{e => println(e._1+"\t"+e._2)}
							// println("Done!")
							this.exit
						}
					//	hash += (key -> elem)			
					case "Hi" =>
						toWait = toWait + 1
				}
			}
		}
	}
	
	class SearchActor(term:String,start:String,end:String,index:String,printer:WaitActor) extends Actor{
		def act(){
			printer ! "Hi"
			val searcher = new IndexSearcher(IndexReader.open(index));
			val query = new QueryParser("location:"+term,new StandardAnalyzer())
			val hits:TopDocs = searcher.search(query.parse("location:"+term),new RangeFilter("created_at",start,end,true,false),1)
			println(start+"\t"+hits.totalHits)
			printer ! (start,hits.totalHits)			
		}
	}
}