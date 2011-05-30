package edu.berkeley.xtrace.reporting;

import java.io.IOException;
import java.io.*;
import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public final class BufferedUdpReporter extends Reporter
{
  NBuffer buffer;
  UdpReporter udpReporter;
  static final int numBuffers = 40;
  static final int bufferSize = 4096;
  public BufferedUdpReporter(UdpReporter udpReporter) {
    this.udpReporter = udpReporter;
    buffer = new NBuffer(udpReporter.localSock, udpReporter.localAddr, udpReporter.localPort, numBuffers, bufferSize);

  }

  public synchronized void close() {
    udpReporter.close();
  }

  @Override
  public synchronized void sendReport(Report r) {
    ByteArrayOutputStream msg = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(msg);
    byte[] report;
    try {
      report = r.toString().getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      report = r.toString().getBytes();
    }
    try {
      out.writeInt(report.length);
      out.write(report);
    } catch (IOException e) {
      return;
    }
    buffer.write(msg.toByteArray());
  }
}
