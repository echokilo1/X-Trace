package edu.berkeley.xtrace.reporting;

import java.lang.management.ManagementFactory;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public final class FileReporter extends Reporter {
  private String filename;
  private DataOutputStream out = null;
  
  FileReporter() {
    filename = System.getProperty("xtrace.filename");
    String hostname = "unknown";
		try {
		  hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {;}
    if (filename == null) {
      int processId = ManagementFactory.getRuntimeMXBean().getName().hashCode();
      String id = String.valueOf(processId);
      if (id.charAt(0) == '-')
        id = id.substring(1);
      filename = "/h/ww2/traces/" + hostname + "." + id + ".txt";
    }

    try {
      out = new DataOutputStream(new FileOutputStream(filename));
    } catch (Exception e) {
      out = null;
      System.out.println("Can't open filestream to file");
      return;
    }
  }

  public synchronized void close() {
    if (out != null) {
      try {
        out.close();
      } catch (IOException e) {}
      out = null;
    }
  }

  public synchronized void flush() {
    if (out != null) {
      try {
        out.flush();
      } catch (IOException e) {}
    }
  }

  public synchronized void sendReport(Report r) {
    if (out != null) {
      try {
        byte[] bytes = r.toString().getBytes("UTF-8");
        out.writeInt(bytes.length);
        out.write(bytes);
      } catch (IOException e) {
        System.out.println("Could not write to file");
      }
    }
  }
  
}
