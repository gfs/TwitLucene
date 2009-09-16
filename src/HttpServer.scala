package com.gstocco.TwitLucene

object HttpServer{
	import java.io.{InputStreamReader,BufferedReader,PrintWriter,BufferedWriter,FileWriter}
	import java.lang.Thread

	import org.apache.commons.httpclient.{HttpClient,UsernamePasswordCredentials,NameValuePair,HttpMethod}
	import org.apache.commons.httpclient.auth.AuthScope
	import org.apache.commons.httpclient.methods.{GetMethod,PostMethod}

	import org.joda.time.{DateTime,DateMidnight}
	import org.joda.time.format.{DateTimeFormat,DateTimeFormatter}

	import scala.io.Source
	import scala.actors._
	import scala.actors.Actor._

	var timeout=1

	var dt:DateTime = new DateTime
	val fmt:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

	var err:PrintWriter = new PrintWriter(new BufferedWriter(new FileWriter("errlog"+fmt.print(dt))))

	var writer:PrintWriter = new PrintWriter(new BufferedWriter(new FileWriter("startup."+fmt.print(dt))))
	var mid:DateTime = dt.plusDays(1)
	mid=mid.minus(mid.getMillisOfDay)

	val client:HttpClient = new HttpClient()

	def main(args: Array[String]){
		if (args.length>0){
			args(0) match{
				case "gardenhose" => getGardenhose
				case "shadow" => if(args.length>1){getShadow(args(1))}
				case "crawlByUser" => if(args.length==3){ setWriter(new PrintWriter(new BufferedWriter(new FileWriter("REST."+args(1)+".Crawl")))); crawlByUser(args(1),Integer.parseInt(args(2)));}
				case _ => println("Please provide an argument: either gardenhose or shadow.")
						  err.println("Please provide and argument: either gardenhose or shadow.")
			}
		}

	}
	
	def crawlByUser(userId:String,levels:Int){
		println("Crawling "+userId)
		if(levels > 0){
			val client:HttpClient = new HttpClient()
			var page = 1
			var results = 1
			while(results>0){
				results=0
				val httpget:GetMethod = new GetMethod("http://twitter.com/statuses/user_timeline.json?screen_name="+userId+"&count=200&page="+page)
				val defaultcreds:UsernamePasswordCredentials = new UsernamePasswordCredentials("gstocco", "password")
				client.getState.setCredentials(new AuthScope("twitter.com", 80, AuthScope.ANY_REALM), defaultcreds)
				try {
					client.executeMethod(httpget)
					var reader:BufferedReader = new BufferedReader(new InputStreamReader(httpget.getResponseBodyAsStream,"UTF-8"))
					var tweet:String =""
					var tacos = true
					while(tacos){
						reader.read.toChar match{
							case -1 =>  	err.print("Got a -1 character from the reader.")
											tacos = false
							// case '\r' =>	println("teh fux")
							// 						push(tweet)//Write tweet to file
							// 						results+=1
							// 						val ind = tweet.indexOf("\"in_reply_to_user_id:\"")
							// 						val substr = tweet.substring(ind+20)
							// 						val ind2 = substr.indexOf(',')
							// 						val idToQueue = tweet.substring(ind+20,ind2)
							// 						if(idToQueue.compareTo("null")!=0){
							// 							crawlByUser(idToQueue,levels-1)
							// 						}
							// 						tweet=""
							// 						timeout=1
							case x:char => 	if(tweet.length>2 && tweet.substring(tweet.length-3,tweet.length).compareTo("},{")==0){
												checkAndPush(tweet.substring(0,tweet.length-1),levels)
												tweet=tweet.substring(tweet.length)+x
												results+=1
											} else if(tweet.length>2 && tweet.substring(tweet.length-3,tweet.length).compareTo("}]")==0){
												checkAndPush(tweet,levels)
												tweet=""
												tacos=false
												results+=1
											}
											else{
												tweet=tweet+x
											}
						}
					}
				}
				catch {
					case e:Exception => e.printStackTrace
				}
				finally {
					err.println("Hit an exception.")
					httpget.releaseConnection()
				}
				page+=1		
			}
		}
	}
	
	def checkAndPush(tweet:String,levels:Int){
		push(tweet)
		println(tweet+" was pushed")
		val ind = tweet.indexOf("\"in_reply_to_screen_name\":")
		var substr = tweet.substring(ind)
		substr = substr.substring(substr.indexOf(':')+1)
		println("wtf "+substr)
		val ind2 = substr.indexOf(',')
		val ind3 = substr.indexOf('"')
		if(substr.substring(0,4).compareTo("null")!=0){
			val idToQueue = substr.substring(ind3+1,ind2-1)
			crawlByUser(idToQueue,levels-1)
		}	
	}

	def getGardenhose{
		val dateSwapActor:dsActor = new dsActor("hose")
		dateSwapActor.start
		setWriter(new PrintWriter(new BufferedWriter(new FileWriter("hose."+fmt.print(dt)))))
		val client:HttpClient = new HttpClient()
		val defaultcreds:UsernamePasswordCredentials = new UsernamePasswordCredentials("user", "password")
		client.getState.setCredentials(new AuthScope("stream.twitter.com", 80, AuthScope.ANY_REALM), defaultcreds)
		client.getParams.setAuthenticationPreemptive(true)
		val httpget:GetMethod = new GetMethod("http://stream.twitter.com/gardenhose.json")
		try {
			client.executeMethod(httpget)
			var reader:BufferedReader = new BufferedReader(new InputStreamReader(httpget.getResponseBodyAsStream,"UTF-8"))
			var tweet:String =""
			while(true){
				reader.read.toChar match{
					case -1 =>   err.print("Got a -1 character from the reader.")
					case '\r' => push(tweet)
					tweet=""
					timeout=1
					case x:char => tweet=tweet+x 
				}
			}		}
		catch {
			case e:Exception => e.printStackTrace
		} 
		finally {
			err.println("Hit an exception. Attempting to restart.")
			httpget.releaseConnection()
			Thread.sleep(timeout)
			timeout=timeout*2
			getGardenhose
		}
	}

	def getShadow(filename:String){
		val dateSwapActor:dsActor = new dsActor("shadow")
		dateSwapActor.start
		setWriter(new PrintWriter(new BufferedWriter(new FileWriter("shadow."+fmt.print(dt)))))
		val defaultcreds:UsernamePasswordCredentials = new UsernamePasswordCredentials("user", "password")
		client.getState.setCredentials(new AuthScope("stream.twitter.com", 80, AuthScope.ANY_REALM), defaultcreds)
		client.getParams.setAuthenticationPreemptive(true)
		val httppost:PostMethod = new PostMethod("http://stream.twitter.com/follow.json")
		try {
			val lines = Source.fromFile(filename,"UTF-8").getLines
			val line = lines.next.toString
			val data:Array[NameValuePair]= new Array[NameValuePair](1)
			data(0)=new NameValuePair("follow",line.substring(7))//cut out the follow=
			httppost.setRequestBody(data);
			client.executeMethod(httppost)
			var reader:BufferedReader = new BufferedReader(new InputStreamReader(httppost.getResponseBodyAsStream,"UTF-8"))
			var tweet:String =""
			while(true){
				reader.read.toChar match{
					case -1 =>   err.print("Got a -1 character from the reader.")
					case '\r' => push(tweet)
					tweet=""
					timeout=1
					case x:char => tweet=tweet+x 
				}
			}		}
		catch {
			case e:Exception => e.printStackTrace
		} 
		finally {
			err.println("Hit an exception. Attempting to restart.")
			httppost.releaseConnection()
			Thread.sleep(timeout)
			timeout=timeout*2
			getShadow(filename)
		}
	}

	def push(input:String){
		writer.print(input)
		writer.flush
	}

	def setWriter(writer:PrintWriter){
		val oldWriter = this.writer
		this.writer = writer
		oldWriter.close
	}

	class dsActor(filePrepend:String) extends Actor{
		def act{
			while (true){
				Thread.sleep(1000)
				if (mid.compareTo(new DateTime)<=0){
					dt=new DateTime
					setWriter(new PrintWriter(new BufferedWriter(new FileWriter(filePrepend+"."+fmt.print(dt)))))
					mid = mid.plusDays(1)
				}
			}
		}
	}
}