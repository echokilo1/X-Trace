package edu.berkeley.xtrace.server;

import edu.berkeley.xtrace.XTraceException;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class XTraceCollector {
  public static void main(String args[]) throws IOException {
    if (args.length < 2) {
      System.out.println("XTraceCollector <report file> <data directory>");
      return;
    }
    BufferedReader in = null;
    in = new BufferedReader(new FileReader(args[0]));
    ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1024);
    System.setProperty("xtrace.server.storedirectory", args[1]);
    FileTreeReportStore store = new FileTreeReportStore();
    store.setReportQueue(queue);
    try {
      store.initialize();
    } catch (XTraceException e) {
      System.out.println("Cannot start report store");
      return;
    }

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(store);
    long syncInterval = 5;
    Timer timer = new Timer();
		timer.schedule(new SyncTimer(store), syncInterval*1000, syncInterval*1000);
    String line = in.readLine();
    while(line != null) {
      String report = "";
      while(line != null && !(line = in.readLine()).equals("X-Trace Report ver 1.0"))
        report += line + '\n';
      report = "X-Trace Report ver 1.0\n" + report;
      while(true) {
        try {
          queue.put(report);
          break;
        } catch (Exception e) {
          continue;
        }
      }
    }
    in.close();
    while(queue.size() > 0)
      ;
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }
    store.shutdown();
    System.exit(0);
  }
	
  private static final class SyncTimer extends TimerTask {
		private QueryableReportStore reportstore;

		public SyncTimer(QueryableReportStore reportstore) {
			this.reportstore = reportstore;
		}

		public void run() {
			reportstore.sync();
		}
	}
}
