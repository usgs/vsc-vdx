package gov.usgs.volcanoes.vdx.in.conn;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.util.StringUtils;
import gov.usgs.volcanoes.vdx.in.hw.Device;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for handling String communication over an IP socket.
 *
 * @author Dan Cervelli
 * @author Loren Antolik
 * @version 1.3
 */
public class IPConnection extends Thread implements Connection {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * the host address of the socket.
   */
  protected String host;

  /**
   * the port number of the socket.
   */
  protected int port;

  /**
   * the connection timeout.
   */
  protected int timeout;

  /**
   * the actual socket for communication.
   */
  private Socket socket;

  /**
   * an output stream for the socket.
   */
  private PrintWriter out;

  /**
   * an input stream for the socket.
   */
  private InputStreamReader in;

  /**
   * a character buffer for reading from the socket.
   */
  private char[] buffer;

  /**
   * the size of the character buffer.
   */
  private static final int BUFFER_SIZE = 2048;

  /**
   * whether or not to stop the thread from executing (used when quitting).
   */
  protected volatile boolean stopThread = true;

  /**
   * whether or not the socket is open.
   */
  protected boolean open;

  /**
   * an ordered message queue.
   */
  protected Vector<String> msgQueue;

  /**
   * a mechanism to lock the queue.  Also used by subclasses.
   */
  protected boolean lockQueue;

  /**
   * default constructor.
   */
  public IPConnection() {
    super("IPConnection");
  }

  /**
   * constructor.
   *
   * @param name thread name
   */
  public IPConnection(String name) {
    super(name);
  }

  /**
   * Initialize IPConnection.
   */
  public void initialize(ConfigFile params) throws Exception {
    host = params.getString("host");
    port = StringUtils.stringToInt(params.getString("port"), Integer.MIN_VALUE);
    timeout = StringUtils.stringToInt(params.getString("timeout"), 30000);
    buffer = new char[BUFFER_SIZE];
    msgQueue = new Vector<String>(100);
  }

  /**
   * Connects to socket for communication.
   */
  public void connect() throws Exception {
    try {
      open();
    } catch (UnknownHostException e) {
      throw new Exception("Unknown host: " + host + ":" + port);
    } catch (IOException e) {
      throw new Exception("Couldn't open socket input/output streams");
    }
  }

  /**
   * Disconnects from socket.
   */
  public void disconnect() {
    close();
  }

  /**
   * Opens the socket for communication.
   *
   * @return whether or not the operation was successful
   * @throws UnknownHostException if the host (IP) can't be found
   * @throws IOException if the socket fails to open
   */
  public boolean open() throws UnknownHostException, IOException {
    socket = new Socket(host, port);
    out = new PrintWriter(socket.getOutputStream());
    in = new InputStreamReader(socket.getInputStream());
    open = true;
    stopThread = false;
    socket.setSoLinger(true, 0);
    if (!this.isAlive()) {
      start();
    }
    return true;
  }

  /**
   * Closes the connection and stops waiting for bytes.  Sets stopThread to true, which silently
   * kills the thread
   */
  public void close() {
    try {
      open = false;
      stopThread = true;
      out.close();
      in.close();
      socket.close();
      msgQueue.removeAllElements();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Thread run() implementation.  Just constantly tries reading bytes from the socket.
   */
  public void run() {
    while (!stopThread) {
      try {
        int count = in.read(buffer);
        receiveData(buffer, count);
      } catch (Exception e) {
        logger.debug("Problem receiving data.", e);
      }
    }
  }

  /**
   * Places received bytes in a message queue.
   *
   * @param buffer the received bytes
   * @param count the number of bytes (note count != data.length)
   */
  private synchronized void receiveData(char[] buffer, int count) {
    lockQueue = true;
    String msg = new String(buffer, 0, count);
    msgQueue.add(msg);
    lockQueue = false;
  }

  /**
   * Writes a string to the socket.
   *
   * @param msg the string
   */
  public void writeString(String msg) throws Exception {

    if (!open) {
      throw new Exception("Connection not open");
    }

    int len = msg.length();
    int idx = 0;
    while (idx < len) {
      out.write((int) msg.charAt(idx++));
    }
    out.flush();
  }

  /**
   * Gets the first message in the queue.
   *
   * @return the message
   */
  public String readString(Device device, int dataTimeout) throws Exception {

    if (!open) {
      throw new Exception("Connection not open");
    }

    // initialize variables
    long start = System.currentTimeMillis();
    long end = start + dataTimeout;
    long now = start;
    long delay = 10;

    if ((dataTimeout > 0) && (dataTimeout < delay)) {
      delay = dataTimeout;
    }

    StringBuffer sb = new StringBuffer();

    // try the message queue while we are within the timeout
    while ((now < end) || (-1L == dataTimeout)) {
      if (!lockQueue) {
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
      }

      // sleep, then try to get more parts of the message
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        logger.error("Interruption while sleeping.", e);
      }

      // update the current time
      now = System.currentTimeMillis();
    }

    // if a complete message wasn't found, then return what was found
    String txt = "Timeout while waiting for data.\n" + sb.toString();
    throw new Exception(txt);
  }

  public String readString(int dataTimeout) throws Exception {
    return readString(null, dataTimeout);
  }

  public String readString(Device device) throws Exception {
    return readString(device, device.getTimeout());
  }

  /**
   * returns status of open variable.
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Get settings.
   */
  public String toString() {
    String settings = "host:" + host + "/";
    settings += "port:" + port + "/";
    settings += "timeout:" + timeout + "/";
    return settings;
  }

  public String getMsgQueue() {
    return msgQueue.toString();
  }

  public void emptyMsgQueue() {
    msgQueue.removeAllElements();
  }
}
