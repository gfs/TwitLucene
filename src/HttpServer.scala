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
				case _ => println("Please provide an argument: either gardenhose or shadow.")
						  err.println("Please provide and argument: either gardenhose or shadow.")
			}
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