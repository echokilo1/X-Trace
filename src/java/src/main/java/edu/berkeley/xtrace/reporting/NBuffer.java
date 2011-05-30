package edu.berkeley.xtrace.reporting;

import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;

public class NBuffer {
  int bufferSize;
  int currentIndex;
  int lostRecords;
  Buffer[] buffers;

  public NBuffer(OutputStream out, int numBuffers, int bufferSize) {
    this.bufferSize = bufferSize;
    currentIndex = 0;
    lostRecords = 0;
    buffers = new Buffer[numBuffers];
    for (int i = 0; i < numBuffers; ++i) {
      buffers[i] = new Buffer(this, out, bufferSize);
    }
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        for(int i = 0; i < buffers.length; i++) {
          buffers[i].shutdown = true;
          buffers[i].flusherThread.interrupt();
          buffers[i].flush();
          Runtime.getRuntime().halt(0);
        }
      }
    });
  }

  public NBuffer(DatagramSocket sock, InetAddress localAddr, int localPort, int numBuffers, int bufferSize) {
    this.bufferSize = bufferSize;
    lostRecords = 0;
    currentIndex = 0;
    buffers = new Buffer[numBuffers];
    for (int i = 0; i < numBuffers; ++i) {
      buffers[i] = new Buffer(this, sock, localAddr, localPort, bufferSize);
    }
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        for(int i = 0; i < buffers.length; i++) {
          buffers[i].shutdown = true;
          buffers[i].flusherThread.interrupt();
          buffers[i].flush();
          Runtime.getRuntime().halt(0);
        }
      }
    });
  }

  public void write(byte[] buf) {
    for(int i = 0; i < buffers.length; i++) {
      Buffer buffer = buffers[(currentIndex + i) % buffers.length];
      buffer.lock.lock();
      if (buffer.bufferFull == 0) {
        if (buf.length > bufferSize - buffer.usedBytes) {
          buffer.bufferFull = 1;
          buffer.threadCond.signal();
          buffer.lock.unlock();
        } else {
          System.arraycopy(buf, 0, buffer.buf, buffer.usedBytes, buf.length);
          buffer.usedBytes += buf.length;
          buffer.lastWrite = System.nanoTime();
          buffer.lock.unlock();
          currentIndex = (currentIndex + i) % buffers.length;
          return;
        }
      }
    }
    lostRecords++;
    System.out.println("Lost " + lostRecords);
    return;
  }

  private static class Buffer {
    public static final long FLUSH_PERIOD = 1000000000;
    int usedBytes;
    boolean shutdown;
    byte[] buf;
    NBuffer parent;
    OutputStream out;
    DatagramSocket sock;
    InetAddress localAddr;
    int port;
    long lastWrite;
    Thread flusherThread;
    Lock lock;
    Condition threadCond;
    int bufferFull;
    boolean isUDP;
    // Thread creation
    // Condition variables

    public Buffer (NBuffer parent, OutputStream out, int bufferSize) {
      buf = new byte[bufferSize];
      usedBytes = 0;
      shutdown = false;
      lastWrite = 0;
      isUDP = false;
      this.parent = parent;
      this.out = out;
      this.sock = null;
      int bufferFull = 0;
      lock = new ReentrantLock();
      threadCond = lock.newCondition();
      flusherThread = new Thread(new FlusherRunnable(this));
      flusherThread.start();
    }
    public Buffer (NBuffer parent, DatagramSocket sock, InetAddress localAddr, int port, int bufferSize) {
      buf = new byte[bufferSize];
      usedBytes = 0;
      shutdown = false;
      lastWrite = 0;
      isUDP = true;
      this.parent = parent;
      this.out = null;
      this.sock = sock;
      this.localAddr = localAddr;
      this.port = port;
      int bufferFull = 0;
      lock = new ReentrantLock();
      threadCond = lock.newCondition();
      flusherThread = new Thread(new FlusherRunnable(this));
      flusherThread.start();
    }
    public class FlusherRunnable implements Runnable {
      Buffer b;
      public FlusherRunnable(Buffer b) { this.b = b; }
      public void run() {
        b.lock.lock();
        try {
          if (b.shutdown) {
            b.lock.unlock();
            return;
          }
          while(!b.shutdown){
            while(!b.shutdown && b.bufferFull == 0) {
              try {
                boolean status = b.threadCond.await(FLUSH_PERIOD, TimeUnit.NANOSECONDS);
                if (!status) {
                  if (b.usedBytes > 0 && System.nanoTime() - lastWrite > FLUSH_PERIOD)
                  break;
                }
              } catch (InterruptedException e) {
                assert(b.shutdown);
                return;
              }
            }
            b.bufferFull = 1;
            b.flush();
          }
        } finally {
          b.lock.unlock();
        }
      }
    }
    public void flush() {
      if (usedBytes > 0) {
        try {
          if (isUDP) {
            DatagramPacket pkt = new DatagramPacket(buf, 0, usedBytes, localAddr, port);
            sock.send(pkt);
          } else {
            out.write(buf, 0, usedBytes);
          }
        } catch (IOException e) {
        }
      }
      bufferFull = 0;
      usedBytes = 0;
    }
  }
}
