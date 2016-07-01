package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import scala.io.Source
import java.io.PrintWriter
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JobLogger

/**
 * @author Santhosh
 */
object ScriptDispatcher extends IDispatcher {
    
    val className = "org.ekstep.analytics.framework.dispatcher.ScriptDispatcher"
    
    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]) : Array[String] = {
        val script = config.getOrElse("script", null).asInstanceOf[String];
        if (null == script) {
            val msg = "'script' parameter is required to send output to file"
            val exp = new DispatcherException(msg)
            JobLogger.log(msg, className, Option(exp), None, Option("FAILED"), "ERROR")
            throw exp;
        }
        val envParams = config.map(f => f._1 + "=" + f._2.asInstanceOf[String]).toArray;
        val proc = Runtime.getRuntime.exec(script, envParams);
        new Thread("stderr reader for " + script) {
            override def run() {
                for (line <- Source.fromInputStream(proc.getErrorStream).getLines)
                    Console.err.println(line)
            }
        }.start();
        new Thread("stdin writer for " + script) {
            override def run() {
                val out = new PrintWriter(proc.getOutputStream)
                for (elem <- events)
                    out.println(elem)
                out.close()
            }
        }.start();
        val outputLines = Source.fromInputStream(proc.getInputStream).getLines;
        val exitStatus = proc.waitFor();
        if (exitStatus != 0) {
            val msg = "Script exited with non zero status"
            val exp = new DispatcherException(msg)
            JobLogger.log(msg, className, Option(exp), None, Option("FAILED"), "ERROR")
            throw exp;
        }
        outputLines.toArray;
    }

}