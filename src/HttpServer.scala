package com.gstocco.TwitLucene

object HttpServer{
	import java.io.{InputStreamReader,BufferedReader,PrintWriter,BufferedWriter,FileWriter}
	import java.lang.Thread

	import org.apache.commons.httpclient.{HttpClient,UsernamePasswordCredentials}
	import org.apache.commons.httpclient.auth.AuthScope
	import org.apache.commons.httpclient.methods.GetMethod

	import org.joda.time.{DateTime,DateMidnight}
	import org.joda.time.format.{DateTimeFormat,DateTimeFormatter}

	import scala.actors._
	import scala.actors.Actor._

	var dt:DateTime = new DateTime
	val fmt:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
	var writer:PrintWriter = new PrintWriter(new BufferedWriter(new FileWriter("tweets."+fmt.print(dt))))

	var mid:DateTime = dt.plusDays(1)
	mid=mid.minus(mid.getMillisOfDay)

	def main(args: Array[String]){
		println(fmt.print(mid))
		val dateSwapActor:dsActor = new dsActor
		dateSwapActor.start
		val client:HttpClient = new HttpClient()
		val defaultcreds:UsernamePasswordCredentials = new UsernamePasswordCredentials("user", "password")
		client.getState.setCredentials(new AuthScope("stream.twitter.com", 80, AuthScope.ANY_REALM), defaultcreds)
		client.getParams.setAuthenticationPreemptive(true)
		val httpget:GetMethod = new GetMethod("http://stream.twitter.com/spritzer.json")
		try {
			client.executeMethod(httpget)
			var reader:BufferedReader = new BufferedReader(new InputStreamReader(httpget.getResponseBodyAsStream,"UTF-8"))
			var tweet:String =""
			while(true){
				reader.read.toChar match{
					case -1 =>
					case '\r' => push(tweet)
					tweet=""
					case x:char => tweet=tweet+x 
				}
			}
		}
		catch {
			case e:Exception => e.printStackTrace
		} 
		finally {
			println("done")
			httpget.releaseConnection()
		}
	}

	def push(input:String){
		writer.print(input)
	}

	def setWriter(writer:PrintWriter){
		this.writer.close
		this.writer = writer
	}

	class dsActor extends Actor{
		def act(){
			while (true){
				Thread.sleep(1000)
				if (mid.compareTo(new DateTime)<=0){
					dt=new DateTime
					setWriter(new PrintWriter(new BufferedWriter(new FileWriter("tweets."+fmt.print(dt)))))
					mid = mid.plusDays(1)
				}
			}
		}
	}
}