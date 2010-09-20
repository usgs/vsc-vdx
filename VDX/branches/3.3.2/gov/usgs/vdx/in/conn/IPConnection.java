package gov.usgs.vdx.in.conn;

import java.net.*;
import java.io.*;
import java.util.*;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.vdx.in.hw.Device;

/**
 * A class for handling String communication over an IP socket.
 *
 * @author Dan Cervelli, Loren Antolik
 * @version 1.3
 */
public class IPConnection extends Thread implements Connection {
	
	/** the host address of the socket */
	protected String host;
	
	/** the port number of the socket */
	protected int port;
	
	/** the connection timout */
	protected int timeout;
	
	/** whether or not to echo incoming bytes from the socket to standard out */
	private boolean echo;
	
	/** the actual socket for communication */
	private Socket socket;
	
	/** an output stream for the socket */
	private PrintWriter out;
	
	/** an input stream for the socket */
	private InputStreamReader in;
	
	/** a character buffer for reading from the socket */	
	private char buffer[];
	
	/** the size of the character buffer */
	private static final int BUFFER_SIZE = 2048;
	
	/** whether or not to stop the thread from executing (used when quitting) */
	protected boolean stopThread = false;
	
	/** whether or not the socket is open */
	protected boolean open;
	
	/** an ordered message queue */
	protected Vector<String> msgQueue;
	
	/** a mechanism to lock the queue.  Also used by subclasses. */
	protected boolean lockQueue;
	
	/**
	 * Initialize IPConnection
	 */
	public void initialize(ConfigFile params) throws Exception {
		host		= params.getString("host");
		port		= Util.stringToInt(params.getString("port"));
		timeout		= Util.stringToInt(params.getString("timeout"), 30000);
		echo		= Util.stringToBoolean(params.getString("echo"), false);
		buffer		= new char[BUFFER_SIZE];
		msgQueue	= new Vector<String>(100);
	}
	
	/**
	 * Get settings
	 */
	public String toString() {
		String settings	= "host:" + host + "/";
		settings	   += "port:" + port + "/";
		settings	   += "timeout:" + timeout + "/";
		return settings;
	}
	
	public void connect() throws Exception {
		try {
			open();
		} catch (UnknownHostException e) {
			throw new Exception("Unknown host: " + host + ":" + port);
		} catch (IOException e) {
			throw new Exception("Couldn't open socket input/output streams");
		}
	}
	
	public void disconnect() {
		close();
	}

	/** Writes a string to the socket.
	 * @param s the string
	 */
	/*
	public void writeString(String s) {
		int len = s.length();
		int idx = 0;
		while (idx < len)
			out.write ((int)s.charAt(idx++));
		
		out.flush();
	}
	*/

	/** 
	 * Writes a string to the socket.
	 * @param s the string
	 */
	public void writeString(String msg) throws Exception {
		if (!open) throw new Exception("Connection not open for writing");
		
		// is a new message arriving
		/*
		if (lockQueue) {
			try { 
				Thread.sleep(timeout);
			} catch (InterruptedException e) {}
			
			if (lockQueue) {
				throw new Exception("Connection locked for reading");
			}
		}
		*/
		
		//lockQueue	= true;
		int len		= msg.length();
		int idx		= 0;
		while (idx < len) {
			out.write((int)msg.charAt(idx++));
		}
		out.flush();
		//lockQueue	= false;
	}
	
	/** Gets the first message in the queue.
	 * @return the message
	 */
	/*
	public synchronized String readString(long timeout) {
		String msg = null;
		if (msgQueue.size() != 0) {
			msg = (String)msgQueue.elementAt(0);
			msgQueue.removeElementAt(0);
		}
		return msg;
	}
	*/
	
	/** Gets the first message in the queue.
	 * @return the message
	 */
	public synchronized String readString(Device device, int dataTimeout) throws Exception {
		
		// verify that the connection is open
		if (!open) {
			throw new Exception("Connection not open");
		}
		
		// initialize variables
		long start		= System.currentTimeMillis();
		long end		= start + dataTimeout;
		long now		= start;
		long delay		= 10;
		
		if ((dataTimeout > 0) && (dataTimeout < delay)) {
			delay	= dataTimeout;
		}
		
		StringBuffer sb	= new StringBuffer();
		
		// try the message queue while we are within the timeout
		while ((now < end) || (-1L == dataTimeout)) {
			if (!msgQueue.isEmpty()) {
				sb.append(msgQueue.elementAt(0));
				msgQueue.removeElementAt(0);
				
				// if the message is complete
				if (device != null) {
					if (device.messageCompleted(sb.toString())) {
						return sb.toString();
					}
				} else {
					return sb.toString();
				}
			}
			
			// sleep, then try to get more parts of the message
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {}
			
			// update the current time
			now = System.currentTimeMillis();
		}
		
		// if a complete message wasn't found, then return what was found
		String txt	= "Timeout while waiting for data.\n" + sb.toString();
		throw new Exception(txt);
	}
	
	public String readString(int dataTimeout) throws Exception {
		return readString(null, dataTimeout);
	}
	
	public String readString(Device device) throws Exception {
		return readString(device, device.getTimeout());
	}
	
	/** Opens the socket for communication.
	 * @throws UnknownHostException if the host (IP) can't be found
	 * @throws IOException if the socket fails to open
	 * @return whether or not the operation was successful
	 */
	public boolean open() throws UnknownHostException, IOException {
		boolean result	= false;
		socket			= new Socket(host, port);
		out				= new PrintWriter(socket.getOutputStream());
		in				= new InputStreamReader(socket.getInputStream());
		result			= true;
		open			= true;
		stopThread		= false;
		if (!this.isAlive()) {
			start();
		}
		return result;
	}
	
	/** Closes the connection and stops waiting for bytes.
	 */
	public void close() {
		try {
			//if (open) {
				open		= false;
				stopThread	= true;
				out.close();
				in.close();
				socket.close();
				msgQueue.removeAllElements();
			//}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/** Gets the open state of this connection.
	 * @return the open state
	 */
	public boolean isOpen() {
		return open;
	}
	
	/** Thread run() implementation.  Just constantly trys reading bytes from the socket.
	 */
	public void run() {
		while (true) {
			
			try {
				if (open && !stopThread) {
					int count = in.read(buffer);
					receiveData(buffer, count);
				} else {
					Thread.sleep(300);
				}
				
			// this occurs whenever the socket closes, this is normal
			} catch (SocketException e) {
				e.printStackTrace();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/** Places received bytes in a message queue.
	 * @param data the received bytes
	 * @param count the number of bytes (note count != data.length)
	 */
	private synchronized void receiveData(char[] data, int count) {
		lockQueue	= true;
		String msg	= new String(data, 0, count);
		msgQueue.add(msg);
		System.out.println("IN BEGIN\n" + msg + "\nIN END");
		lockQueue	= false;
	}
}
