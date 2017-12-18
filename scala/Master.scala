/*import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import akka.pattern.ask
import akka.dispatch.ExecutionContexts._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContextExecutor, Future}


//继承Akka的actor
class Master extends Actor {

  implicit val timeout = Timeout.apply(1L, TimeUnit.HOURS)

  val futures = new ArrayBuffer[Future[Any]]

  val results = new ArrayBuffer[Result]

  //重新receive方法用于接收消息
  override def receive: Receive = {

    case SubmitTask(path) => {
      //读取该目录小的文件
      val file = new File(path)
      val files: Array[File] = file.listFiles()
      //for循环，有几个文件就创建几个Worker
      //创建Worker，并提交任务，返回future
      for(f <- files; if f.isFile) {
        //通过Master Actor的上下文创建Worker Actor
        val workerRef = context.actorOf(Props[Worker])
        //向Work发送计算某个文件的任务
        val future: Future[Any] = workerRef ? Task(f)
        //将future保存起来
        futures += future
      }

      //如果还有没计算的future，那么就一直计算
      while(futures.length > 0) {
        //拿出完成计算的future
        val dones = futures.filter(_.isCompleted)
        //循环
        for(done <- dones) {
          //得到结果
          val r: Result = done.value.get.get.asInstanceOf[Result]
          results += r
          //移除已经计算好的future
          futures -= done
        }
        Thread.sleep(500)
      }

      //第一次foreach取出来的future
            implicit val executionContextExecutor = global()
            futures.foreach(f => {
              //取出计算好的结果
              f.foreach(r => {
                results += r.asInstanceOf[Result]
              })
            })

      //最后统计所有的结果
      val finalR = results.flatMap(_.r.toList).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))

      //将结果回传给driver
      sender() ! finalR
    }

  }
}

//伴生对象，用于创建ActorSystem
object Master {

  def main(args: Array[String]): Unit = {
    implicit val timeout = Timeout.apply(1L, TimeUnit.HOURS)
    //1.创建ActorSystem
    val actorSystem: ActorSystem = ActorSystem("ActorSystem")

    //2.创建Actor（Master）,并给Actor取一个名字
    //放回的是Actor的引用
    //启动进程并一直运行用来接收消息

    val host = "192.168.14.33"
    val port = "8888".toInt

    //创建一个配置文件字符串(ip,port)
    val confStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = $host
         |akka.remote.netty.tcp.port = $port
      """.stripMargin

    val conf = ConfigFactory.parseString(confStr)
    //创建ActorSystem，指定了其配置信息（ip、端口、通信的实现类）
    val masterActorSystem = ActorSystem("master_system", conf)
    val masterRef: ActorRef = actorSystem.actorOf(Props(new Master), "Master")

    //3.拿到了Master的引用，并向Master发消息

    //传入一个隐式参数


    //发送异步消息，但是返回一个Future
    val future: Future[Any] = masterRef ? SubmitTask("E:/wc")

    implicit val executionContextExecutor = global()
    future.foreach(r => {
      println(r)
    })
    //停掉计算任务
    actorSystem.terminate()


  }

}*/

/*import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._

class Master extends Actor{

  val id2Workers = new mutable.HashMap[String, WorkerInfo]()

  val CHECK_INTERVAL = 15000

  //Master创建之后就启动一个定时器，用来检测超时的Worker
  override def preStart(): Unit = {
    //导入隐式转换
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, CheckTimeOutWorker)
  }

  override def receive: Receive = {

    //Worker发送给Master的注册消息
    case RegisterWorker(workerId, memory, cores) => {
      //将注册消息保存起来
      val workerInfo = new WorkerInfo(workerId, memory, cores)
      //保存到集合
      println(workerId)
      id2Workers(workerId) = workerInfo
      //返回一个一个消息告诉Worker注册成功了
      sender() ! RegisteredWorker
    }

    //Worker发送给Master的心跳信息
    case Heartbeat(workerId) => {
      //根据workerId到保存worker信息的map中查找
      if(id2Workers.contains(workerId)) {
        val workerInfo: WorkerInfo = id2Workers(workerId)
        //更新Worker的状态（上一次心跳的时间）
        val current = System.currentTimeMillis()
        workerInfo.lastHeartbeatTime = current
      }
    }

    case CheckTimeOutWorker => {
      //
      val current = System.currentTimeMillis();
      //过滤出超时的Worker
      val deadWorkers = id2Workers.values.filter(w => current - w.lastHeartbeatTime > CHECK_INTERVAL)
      //移除超时的worker
      //for(w <- deadWorkers) {
      //  id2Workers -= w.workerId
      //}
      deadWorkers.foreach(dw => {
        id2Workers -= dw.workerId
      })
      println("current works size : " + id2Workers.size)
    }


  }
}

object Master {

  val MASTER_SYSTEM = "MasterSystem"
  val MASTER_NAME = "Master"


  def main(args: Array[String]): Unit = {


    //通过ActorSystem创建Actorval host = "192.168.14.33"
    val port = "8888".toInt

    //创建一个配置文件字符串(ip,port)
    val confStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = $host
         |akka.remote.netty.tcp.port = $port
      """.stripMargin

    val conf = ConfigFactory.parseString(confStr)
    //创建ActorSystem，指定了其配置信息（ip、端口、通信的实现类）
    val masterActorSystem = ActorSystem(MASTER_SYSTEM, conf)
    masterActorSystem.actorOf(Props[Master], MASTER_NAME)

    masterActorSystem.whenTerminated

  }
}*/


import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.Future
import scala.sys.Prop
import akka.pattern.ask
import akka.util.Timeout
import akka.dispatch.ExecutionContexts._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}



//case class Task(file:File)
//case class SubmitTask(path:String)
//case class Result(r:Map[String,Int])




class Master extends Actor{

  val id2Workers = new mutable.HashMap[String, WorkerInfo]()
  val CHECK_INTERVAL = 15000

  implicit val timeout = Timeout.apply(1L, TimeUnit.HOURS)
  val futures = new ArrayBuffer[Future[Any]]
  val results = new ArrayBuffer[Result]

  //Master创建之后就启动一个定时器，用来检测超时的Worker
  override def preStart(): Unit = {
    //导入隐式转换
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, CheckTimeOutWorker)
  }

  override def receive: Receive = {
//    case "hello"=>{
//      println("Master 管理")
//    }
    case "connect" => {
      print("a client connected")
      sender ! "reply"
    }

    //Worker发送给Master的注册消息
    case RegisterWorker(workerId, memory, cores) => {
      //将注册消息保存起来
      println(workerId)
      val workerInfo = new WorkerInfo(workerId, memory, cores)
      //保存到集合
      id2Workers(workerId) = workerInfo
      //返回一个一个消息告诉Worker注册成功了
      sender() ! RegisteredWorker


      val file = new File("E://wc")
      val files:Array[File] = file.listFiles()

      for(f<-files;if f.isFile){
        //创建Actor
        val future:Future[Any] = sender() ? Task(f)

        futures += future

      }
      //如果还有没计算的future，那么就一直计算
      while(futures.length > 0) {
        //拿出完成计算的future
        val dones = futures.filter(_.isCompleted)
        //循环
        for(done <- dones) {
          //得到结果
          val r: Result = done.value.get.get.asInstanceOf[Result]
          results += r
          //移除已经计算好的future
          futures -= done
        }
        Thread.sleep(500)
      }

      //最后统计所有的结果
      val finalR = results.flatMap(_.r.toList).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))

      //将结果回传给driver
      //      sender() ! finalR
      println(finalR)





    }

    //Worker发送给Master的心跳信息
    case Heartbeat(workerId) => {
      //根据workerId到保存worker信息的map中查找
      if (id2Workers.contains(workerId)) {
        val workerInfo: WorkerInfo = id2Workers(workerId)
        //更新Worker的状态（上一次心跳的时间）
        val current = System.currentTimeMillis()
        workerInfo.lastHeartbeatTime = current
      }
    }
    case CheckTimeOutWorker => {
      //
      val current = System.currentTimeMillis();
      //过滤出超时的Worker
      val deadWorkers = id2Workers.values.filter(w => current - w.lastHeartbeatTime > CHECK_INTERVAL)
      //移除超时的worker
      //for(w <- deadWorkers) {+?
      //  id2Workers -= w.workerId
      //}
      deadWorkers.foreach(dw => {
        id2Workers -= dw.workerId
      })
      println("current works size : " + id2Workers.size)
    }

    //    case SubmitTask(path) =>{
    //      val file = new File(path)
    //      val files:Array[File] = file.listFiles()
    //
    //      for(f<-files;if f.isFile){
    //        //创建Actor
    //        val future:Future[Any] = sender() ? Task(f)
    //
    //        futures += future
    //
    //      }
    //
    //      //如果还有没计算的future，那么就一直计算
    //      while(futures.length > 0) {
    //        //拿出完成计算的future
    //        val dones = futures.filter(_.isCompleted)
    //        //循环
    //        for(done <- dones) {
    //          //得到结果
    //          val r: Result = done.value.get.get.asInstanceOf[Result]
    //          results += r
    //          //移除已经计算好的future
    //          futures -= done
    //        }
    //        Thread.sleep(500)
    //      }
    //
    //      //最后统计所有的结果
    //      val finalR = results.flatMap(_.r.toList).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))
    //
    //      //将结果回传给driver
    //      sender() ! finalR
    //
    //
    //
    //    }

  }
}

object Master {

  def main(args: Array[String]): Unit = {
    implicit val timeout = Timeout.apply(1L,TimeUnit.HOURS)
    val host = args(0)
    val port = args(1).toInt

    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
    """.stripMargin

    println(configStr)

    val config = ConfigFactory.parseString(configStr)
    //actorSystem
    val actorSystem = ActorSystem("MasterSystem", config)
    val masterRef = actorSystem.actorOf(Props[Master],"Master")
    masterRef ! "hello"
    //结束
    actorSystem.awaitTermination()
  }



}
