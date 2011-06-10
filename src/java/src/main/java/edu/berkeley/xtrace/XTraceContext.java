/*
 * Copyright (c) 2005,2006,2007 The Regents of the University of California.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the University of California, nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF CALIFORNIA ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package edu.berkeley.xtrace;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * High-level API for maintaining a per-thread X-Trace context (task and
 * operation ID) and reporting events.
 * 
 * Usage:
 * <ul>
 * <li> When communication is received, set the context using 
 *      <code>XTraceContext.setThreadContext()</code>.
 * <li> To record an operation, call <code>XTraceContext.logEvent()</code>. 
 *      Or, to add extra fields to the event report, call 
 *      <code>XTraceContext.createEvent()</code>, add fields to the
 *      returned {@link XTraceEvent} object, and send it using 
 *      {@link XTraceEvent#sendReport()}.
 * <li> When calling another service, get the current context's metadata using
 *      <code>XTraceContext.getThreadContext()</code> and send it to the
 *      destination service as a field in your network protocol.
 *      After receiving a reply, add an edge from both the reply's metadata and
 *      the current context in the report for the reply.
 * <li> Clear the context using <code>XTraceContext.clearThreadContext()</code>. 
 * </ul>
 * 
 * @author Matei Zaharia <matei@berkeley.edu>
 */
public class XTraceContext {
	/** Thread-local current operation context, used in logEvent. **/
	private static ThreadLocal<XTraceMetadata> context
		= new ThreadLocal<XTraceMetadata>() {
		@Override
		protected XTraceMetadata initialValue() {
			return null;
		}
	};

  private static ThreadLocal<Integer> refs
    = new ThreadLocal<Integer>() {
      @Override
      protected Integer initialValue() {
        return 0;
      }
    };

  private static ThreadLocal<Random> random
    = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
			int processId = ManagementFactory.getRuntimeMXBean().getName().hashCode();
			try {
				return new Random(++threadId
						+ processId
						+ System.nanoTime()
						+ Thread.currentThread().getId()
						+ InetAddress.getLocalHost().getHostName().hashCode() );
			} catch (UnknownHostException e) {
				// Failed to get local host name; just use the other pieces
				return new Random(++threadId
						+ processId
						+ System.nanoTime()
						+ Thread.currentThread().getId());
			}
    }
  };

  private final static long sampleRate = 10;
  private final static long delayAmount = 0;
  private static volatile long threadId = 0;

	/** Cached hostname of the current machine. **/
	private static String hostname = null;

	static int defaultOpIdLength = 8;
	
	/**
	 * Set the X-Trace context for the current thread, to link it causally to
	 * events that may have happened in a different thread or on a different host.
	 * 
	 * @param ctx the new context
	 */
	public synchronized static void setThreadContext(XTraceMetadata ctx) {
		if (ctx != null && ctx.isValid()) {
			context.set(ctx);
		} else {
			context.set(null);
		}
	}
	
	/**
	 * Get the current thread's X-Trace context, that is, the metadata for the
	 * last event to have been logged by this thread.
	 * 
	 * @return current thread's X-Trace context
	 */
	public synchronized static XTraceMetadata getThreadContext() {
		return context.get();
	}
	
	/**
	 * Clear current thread's X-Trace context.
	 */
	public synchronized static void clearThreadContext() {
		context.set(null);
	}

	/**
	 * Creates a new task context, adds an edge from the current thread's context,
	 * sets the new context, and reports it to the X-Trace server.
	 * 
	 * @param agent name of current agent
	 * @param label description of the task
	 */
	public static void logEvent(String agent, String label) {
    //Bug, might return null
		createEvent(agent, label).sendReport();
	}

	/**
	 * Creates a new task context, adds an edge from the current thread's context,
	 * sets the new context, and reports it to the X-Trace server.
	 * This version of this function allows extra event fields to be specified
	 * as variable arguments after the agent and label. For example, to add a
	 * field called "DataSize" with value 4320, use
	 * 
	 * <code>XTraceContext.logEvent("agent", "label", "DataSize" 4320)</code>
	 * 
	 * @param agent name of current agent
	 * @param label description of the task
	 */
	public static void logEvent(String agent, String label, Object... args) {
		if (context.get()==null) {
			return;
		}
		if (args.length % 2 != 0) {
			throw new IllegalArgumentException(
					"XTraceContext.logEvent requires an even number of arguments.");
		}
		XTraceEvent event = createEvent(agent, label);
		for (int i=0; i<args.length/2; i++) {
			String key = args[2*i].toString();
			String value = args[2*i + 1].toString();
			event.put(key, value);
		}
		event.sendReport();
	}

  public static XTraceEvent createEvent(String agent, String label) {
    return createEvent(agent, label, true);
  }
	
	/**
	 * Creates a new event context, adds an edge from the current thread's
	 * context, and sets the new context. Returns the newly created event without
	 * reporting it. If there is no current thread context, nothing is done.
	 * 
	 * The returned event can be sent with {@link XTraceEvent#sendReport()}.
	 * 
	 * @param agent name of current agent
	 * @param label description of the task
	 */
	public static XTraceEvent createEvent(String agent, String label, boolean newMethod) {
		if (context.get()==null) {
			return null;
		}

		XTraceMetadata oldContext = getThreadContext();
		
		int opIdLength = defaultOpIdLength;
		if (oldContext != null) {
			opIdLength = oldContext.getOpIdLength();
		}
		XTraceEvent event = new XTraceEvent(opIdLength);
    if (newMethod) {
	    event.setMetadata(oldContext);
      if (oldContext.previous != null) {
        event.put("Edge", oldContext.previous.getOpIdString());
        oldContext.previous = null;
      }
    } else {
      event.addEdge(oldContext);
    }

		try {
			if (hostname == null) {
				hostname = InetAddress.getLocalHost().getHostName();
			}
		} catch (UnknownHostException e) {
			hostname = "unknown";
		}

		event.put("Host", hostname);
		event.put("Agent", agent);
		event.put("Label", hostname.toUpperCase() + "_" + label);

		if (!newMethod)
      setThreadContext(event.getNewMetadata());
		return event;
	}

	/**
	 * Is there a context set for the current thread?
	 * 
	 * @return true if there is a current context
	 */
	public static boolean isValid() {
		return getThreadContext() != null;
	}
	
	/**
	 * Begin a "process", which will be ended with 
	 * {@link #endProcess(XTraceProcess)}, by creating an event
	 * with the given agent and label strings. This function returns an
	 * XtrMetadata object that must be passed into 
	 * {@link #endProcess(XTraceProcess)} or 
	 * {@link #failProcess(XTraceProcess, Throwable)} to create the corresponding 
	 * process-end event.
	 * 
	 * Example usage:
	 * <pre>
	 * XtraceProcess process = XTrace.startProcess("node", "action start");
	 * ...
	 * XTrace.endProcess(process);
	 * </pre>
	 * 
	 * The call to {@link #endProcess(XTraceProcess)} will
	 * create an edge from both the start context and the current X-Trace
	 * context, forming a subprocess box on the X-Trace graph.
	 * 
	 * @param agent name of current agent
	 * @param process  name of process
	 * @return the process object created
	 */
	public static XTraceProcess startProcess(String agent, String process, Object... args) {
		logEvent(agent, process + " start", args);
		return new XTraceProcess(getThreadContext(), agent, process);
	}

	
	/**
	 * Log the end of a process started with
	 * {@link #startProcess(String, String)}. 
	 * See {@link #startProcess(String, String)} for example usage.
	 * 
	 * The call to {@link #endProcess(XTraceProcess)} will
	 * create an edge from both the start context and the current X-Trace
	 * context, forming a subprocess box on the X-Trace graph.
	 * 
	 * @see XTraceContext#startProcess(String, String)
	 * @param process return value from #startProcess(String, String)
	 */
	public static void endProcess(XTraceProcess process) {
		endProcess(process, process.name + " end");
	}
	
	/**
	 * Log the end of a process started with
	 * {@link #startProcess(String, String)}. 
	 * See {@link #startProcess(String, String)} for example usage.
	 * 
	 * The call to {@link #endProcess(XTraceProcess, String)} will
	 * create an edge from both the start context and the current X-Trace
	 * context, forming a subprocess box on the X-Trace graph. This version
	 * of the function lets the user set a label for the end node.
	 * 
	 * @see #startProcess(String, String)
	 * @param process return value from #startProcess(String, String)
	 * @param label label for the end process X-Trace node
	 */
	public static void endProcess(XTraceProcess process, String label) {
		if (getThreadContext() != null) {
			XTraceMetadata oldContext = getThreadContext();
			XTraceEvent evt = createEvent(process.agent, label);
			if (oldContext != process.startCtx) {
				evt.addEdge(process.startCtx);	// Make sure we don't get a double edge from startCtx
			}
			evt.sendReport();
		}
	}
	
	/**
	 * Log the end of a process started with
	 * {@link #startProcess(String, String)}. 
	 * See {@link #startProcess(String, String)} for example usage.
	 * 
	 * The call to {@link #failProcess(XTraceProcess, Throwable)} will
	 * create an edge from both the start context and the current X-Trace
	 * context, forming a subprocess box on the X-Trace graph. This version
	 * of the function should be called when a process fails, to report
	 * an exception. It will add an Exception field to the X-Trace report's
	 * metadata.
	 * 
	 * @see #startProcess(String, String)
	 * @param process return value from #startProcess(String, String)
	 * @param exception reason for failure
	 */
	public static void failProcess(XTraceProcess process, Throwable exception) {
		if (getThreadContext() != null) {
			XTraceMetadata oldContext = getThreadContext();
			XTraceEvent evt = createEvent(process.agent, process.name + " failed");
			if (oldContext != process.startCtx) {
				evt.addEdge(process.startCtx);	// Make sure we don't get a double edge from startCtx
			}

			// Write stack trace to a string buffer
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			exception.printStackTrace(pw);
			pw.flush();

			evt.put("Exception", IoUtil.escapeNewlines(sw.toString()));
			evt.sendReport();
		}
	}


	public static void failProcess(XTraceProcess process, String reason) {
		if (getThreadContext() != null) {
			XTraceMetadata oldContext = getThreadContext();
			XTraceEvent evt = createEvent(process.agent, process.name + " failed");
			if (oldContext != process.startCtx) {
				evt.addEdge(process.startCtx);	// Make sure we don't get a double edge from startCtx
			}
			evt.put("Reason", reason);
			evt.sendReport();
		}
	}

	public static void startTrace(String agent, String title, String... tags) {
    //TODO: change
		TaskID taskId = new TaskID(8);
		setThreadContext(new XTraceMetadata(taskId, random.get().nextLong()));
		XTraceEvent event = createEvent(agent, "Start Trace: " + title);
		event.put("Title", title);
		for (String tag: tags) {
			event.put("Tag", tag);
		}
		event.sendReport();
	}

  /* Request level sampling */
  
  public static void newTrace() {
    if (context.get() != null && refs.get() > 0) {
      refs.set(refs.get() + 1);
      return;
    }
    if (random.get().nextInt(100) < sampleRate) {
      TaskID taskId = new TaskID(8);
      //TODO: change
      setThreadContext(new XTraceMetadata(taskId, random.get().nextLong()));
      refs.set(1);
    }
    else {
      setThreadContext(null);
      refs.set(0);
    }
  }

  public static void endTrace() {
    if (context.get() != null)
      refs.set(refs.get() - 1);
    if (refs.get() < 1) {
      setThreadContext(null);
      refs.set(0);
    }
  }

  /* Function calls */
  public static void callStart(String agent, String name) {
    //System.out.println("callStart start");
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_" + name.toUpperCase() + "_START");
    event.sendReport();
    //System.out.println("callStart end");
    //return event.getNewMetadata();
  }

  public static void callEnd(String agent, String name) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_" + name.toUpperCase() + "_END");
    event.sendReport();
    //return event.getNewMetadata();
  }

  public static void callError(String agent, String name) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_" + name.toUpperCase() + "_ERROR");
    event.sendReport();
    //return event.getNewMetadata();
  }

  /* RPC */

  public static XTraceMetadata[] rpcStart(int numCalls) {
    if (numCalls > 1)
      return fork(numCalls, "RPC_CALL");
    if (context.get()==null)
      return new XTraceMetadata[] {null};// null;
    XTraceEvent event = new XTraceEvent(getThreadContext().getOpIdLength());
    
    event.setMetadata(getThreadContext());
    if (getThreadContext().previous != null) {
      event.put("Edge", getThreadContext().previous.getOpIdString());
      getThreadContext().previous = null;
    }
    
    try {
	    if (hostname == null) {
        hostname = InetAddress.getLocalHost().getHostName();
		  }
	  } catch (UnknownHostException e) {
      hostname = "unknown";
	  }
    event.put("Host", hostname);
    event.put("Label", hostname.toUpperCase() + "_RPC_REPLY");
    event.put("Status", "SUCCESS");
    event.sendReport();
    return new XTraceMetadata[] {getThreadContext()};
    //setThreadContext(event.getNewMetadata());
    //return event.getNewMetadata();
  }

  public static void rpcSuccess() {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = new XTraceEvent(getThreadContext().getOpIdLength());
    
    event.setMetadata(getThreadContext());
    if (getThreadContext().previous != null) {
      event.put("Edge", getThreadContext().previous.getOpIdString());
      getThreadContext().previous = null;
    }
    
    try {
	    if (hostname == null) {
        hostname = InetAddress.getLocalHost().getHostName();
		  }
	  } catch (UnknownHostException e) {
      hostname = "unknown";
	  }
    event.put("Host", hostname);
    event.put("Label", hostname.toUpperCase() + "_RPC_REPLY");
    event.put("Status", "SUCCESS");
    event.sendReport();
    //setThreadContext(event.getNewMetadata());
    //return event.getNewMetadata();
  }
  
  public static void rpcError() {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = new XTraceEvent(getThreadContext().getOpIdLength());
    //event.addEdge(getThreadContext());
    event.setMetadata(getThreadContext());
    if (getThreadContext().previous != null) {
      event.put("Edge", getThreadContext().previous.getOpIdString());
      getThreadContext().previous = null;
    }
    try {
	    if (hostname == null) {
        hostname = InetAddress.getLocalHost().getHostName();
		  }
	  } catch (UnknownHostException e) {
      hostname = "unknown";
	  }
    event.put("Host", hostname);
    event.put("Label", hostname.toUpperCase() + "_RPC_REPLY");
    event.put("Status", "ERROR");
    event.sendReport();
    //setThreadContext(event.getNewMetadata());
    //return event.getNewMetadata();
  }
  
  public static void rpcException() {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = new XTraceEvent(getThreadContext().getOpIdLength());
    //event.addEdge(getThreadContext());
    event.setMetadata(getThreadContext());
    if (getThreadContext().previous != null) {
      event.put("Edge", getThreadContext().previous.getOpIdString());
      getThreadContext().previous = null;
    }
    try {
	    if (hostname == null) {
        hostname = InetAddress.getLocalHost().getHostName();
		  }
	  } catch (UnknownHostException e) {
      hostname = "unknown";
	  }
    event.put("Host", hostname);
    event.put("Label", hostname.toUpperCase() + "_RPC_REPLY");
    event.put("Status", "FATAL");
    event.sendReport();
    //setThreadContext(event.getNewMetadata());
    //return event.getNewMetadata();
  }

  /* Cache */
  public static void cacheSearch(String agent, String cacheType) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_" + cacheType.toUpperCase() + "_CACHE_LOOKUP");
    event.sendReport();
    //return event.getNewMetadata();
  }
  
  public static void cacheHit(String agent, String cacheType) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_" + cacheType.toUpperCase() + "_CACHE_HIT");
    event.sendReport();
    //return event.getNewMetadata();
  }
  
  public static void cacheMiss(String agent, String cacheType) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_" + cacheType.toUpperCase() + "_CACHE_MISS");
    event.sendReport();
    //return event.getNewMetadata();
  }

  /* Data Transfer */
  public static XTraceMetadata sendPacket(String agent, long seqno, XTraceMetadata previousSend) {
    if (context.get()==null)
      return null;
    XTraceEvent event;
    if (previousSend == null)
      event = createEvent(agent, agent.toUpperCase() + "_SEND_PACKET");
    else {
      event = createEvent(agent, agent.toUpperCase() + "_SEND_PACKET", false);
      event.addEdge(previousSend);
    }
    event.put("Seqno", String.valueOf(seqno));
    event.sendReport();
    return event.getNewMetadata();
  }
  public static void receivePacket(String agent, long seqno) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_RECEIVE_PACKET");
    event.put("Seqno", String.valueOf(seqno));
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static XTraceMetadata sendAck(String agent, long seqno, XTraceMetadata previousAck) {
    if (context.get()==null)
      return null;
    XTraceEvent event;
    if (previousAck == null)
      event = createEvent(agent, agent.toUpperCase() + "_SEND_ACK");
    else {
      event = createEvent(agent, agent.toUpperCase() + "_SEND_ACK", false);
      event.addEdge(previousAck);
    }
    event.put("Seqno", String.valueOf(seqno));
    event.sendReport();
    return event.getNewMetadata();
  }
  public static XTraceMetadata acceptAck(String agent, long seqno, XTraceMetadata previousAccept) {
    if (context.get()==null)
      return null;
    XTraceEvent event;
    if (previousAccept == null)
      event = createEvent(agent, agent.toUpperCase() + "_ACCEPT_ACK");
    else {
      event = createEvent(agent, agent.toUpperCase() + "_ACCEPT_ACK", false);
      event.addEdge(previousAccept);
    }
    event.put("Seqno", String.valueOf(seqno));
    event.sendReport();
    return event.getNewMetadata();
  }
  public static void receiveAck(String agent, long seqno) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_RECEIVE_ACK");
    event.put("Seqno", String.valueOf(seqno));
    event.sendReport();
    //return event.getNewMetadata();
  }

  /* Data Transfer Ops */
  public static void opReadBlockRequest(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_READ_BLOCK_REQUEST");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void opReadBlockReceive(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_READ_BLOCK_RECEIVE");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void opReadBlockReply(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_READ_BLOCK_REPLY");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void opReadBlockSuccess(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_READ_BLOCK_SUCCESS");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void opReadBlockFailure(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_READ_BLOCK_FAIL");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void opWriteBlockRequest(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_WRITE_BLOCK_REQUEST");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void opWriteBlockReceive(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_WRITE_BLOCK_RECEIVE");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void opWriteBlockReply(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_WRITE_BLOCK_REPLY");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void opWriteBlockSuccess(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_WRITE_BLOCK_SUCCESS");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void opWriteBlockFailure(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_OP_WRITE_BLOCK_FAIL");
    event.sendReport();
    //return event.getNewMetadata();
  }

  /* Write */
  public static void newBlock(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_NEW_BLOCK");
    event.sendReport();
    //return event.getNewMetadata();
  }
  public static void appendBlock(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_APPEND_BLOCK");
    event.sendReport();
    //return event.getNewMetadata();
  }

  public static void endBlock(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_END_BLOCK");
    event.sendReport();
    //return event.getNewMetadata();
  }
  
  public static void writeError(String agent) {
    if (context.get()==null)
      return;// null;
    XTraceEvent event = createEvent(agent, agent.toUpperCase() + "_WRITE_ERROR");
    event.sendReport();
    //return event.getNewMetadata();
  }

  /* Misc */
  public static XTraceMetadata[] fork(int numCalls, String label) {
    if (numCalls < 1)
      return null;
    XTraceMetadata metadata[] = new XTraceMetadata[numCalls];
    if (context.get()==null) {
      for(int i = 0; i < metadata.length; i++)
        metadata[i] = null;
      return metadata;
    }
    try {
	    if (hostname == null) {
        hostname = InetAddress.getLocalHost().getHostName();
		  }
	  } catch (UnknownHostException e) {
      hostname = "unknown";
	  }
    for (int i = 0; i < numCalls; i++) {
      XTraceEvent event = new XTraceEvent(getThreadContext().getOpIdLength());
      event.addEdge(getThreadContext());
      event.put("Label", hostname.toUpperCase() + "_" + label);
      event.put("Host", hostname);
      event.sendReport();
      metadata[i] = event.getNewMetadata();
    }
    return metadata;
  }

  public static XTraceMetadata join(XTraceMetadata[] metadata, String label) {
    if (metadata.length < 1)
      return null;
    if (metadata.length == 1) {
      setThreadContext(metadata[0]);
      return metadata[0];
    }
    XTraceEvent event = new XTraceEvent(metadata[0].getOpIdLength());
    try {
	    if (hostname == null) {
        hostname = InetAddress.getLocalHost().getHostName();
		  }
	  } catch (UnknownHostException e) {
      hostname = "unknown";
	  }
    event.put("Host", hostname);
    event.put("Label", hostname.toUpperCase() + "_" + label);
    for (int i = 0; i < metadata.length; i++)
      if (metadata[i] != null && metadata[i].isValid())
        event.addEdge(metadata[i]);
    event.sendReport();
    setThreadContext(event.getNewMetadata());
    return event.getNewMetadata();
  }

	public static int getDefaultOpIdLength() {
		return defaultOpIdLength;
	}

	public static void setDefaultOpIdLength(int defaultOpIdLength) {
		XTraceContext.defaultOpIdLength = defaultOpIdLength;
	}

	public static void writeThreadContext(DataOutput out) throws IOException {
		XTraceMetadata.write(getThreadContext(), out);
	}
	
	public static void readThreadContext(DataInput in) throws IOException {
		setThreadContext(XTraceMetadata.read(in));
	}
	
	/**
	 * Replace the current context with a new one, returning the value of
	 * the old context.
	 * 
	 * @param newContext The context to replace the current one with.
	 * @return
	 */
	public synchronized static XTraceMetadata switchThreadContext(XTraceMetadata newContext) {
		XTraceMetadata oldContext = getThreadContext();
		setThreadContext(newContext);
		return oldContext;
	}

  public static void newOpId() {
    XTraceMetadata metadata = new XTraceMetadata(getThreadContext().getTaskId(), random.get().nextLong());
    metadata.previous = getThreadContext();
    setThreadContext(metadata);
  }

}
