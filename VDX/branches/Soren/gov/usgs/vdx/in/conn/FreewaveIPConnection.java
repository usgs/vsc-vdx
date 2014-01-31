package gov.usgs.vdx.in.conn;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;

import java.text.*;

/**
 * An extension of IPConnection for dealing with FreeWave communication over IP.
 *
 * @author Dan Cervelli, Ralf Krug, Loren Antolik
 * @version 1.3
 */
public class FreewaveIPConnection extends IPConnection implements Connection {
	
	/** the call number of the freewave */
	protected int callnumber;
	
	/** the repeater entry for the freewave */
	protected int repeater;
	
	/** a number format for properly dialing the radio phone number */
	private DecimalFormat radioNumberFormatter;
	
	/** 
	 * default constructor
	 */
	public FreewaveIPConnection() {
		super();
	}
	
	/**
	 * constructor
	 * @param name thread name
	 */
	public FreewaveIPConnection(String name) {
		super(name);
	}
	
	/**
	 * Initialize FreewaveIPConnection
	 */
	public void initialize(ConfigFile params) throws Exception {
		super.initialize(params);
		callnumber				= Util.stringToInt(params.getString("callnumber"));
		repeater				= Util.stringToInt(params.getString("repeater"));
		radioNumberFormatter	= new DecimalFormat ("#######");
	}
	
	/**
	 * Get settings
	 */
	public String toString() {
		String settings	= super.toString();
		settings	   += "callnumber:" + callnumber + "/";
		settings	   += "repeater:" + repeater + "/";
		return settings;
	}
	
	/** 
	 * Connects to a FreeWave.
	 * @param repeaterEntry the repeater entry number in the FreeWave callbook
	 * @param radioNumber the radio phone number
	 * @param timeout the timeout in milliseconds (-1 == none)
	 * @throws Exception various exceptions can be thrown with different messages depending on the outcome
	 */
	public void connect() throws Exception {
		super.connect();		 
		setRepeater(repeater);
		call(callnumber);
	}
	
	/** 
	 * Calls the FreeWave.
	 * @param radioNumber the radio phone number
	 * @param establishConnectionTimeout the timeout (ms)
	 */
	private void call(int radioNumber) throws Exception {
		String cmd = "ATD" + radioNumberFormatter.format(radioNumber);
		writeString(cmd);
		
		// throws FreewaveConnectionException if no 'OK' answer
		wait4OK(timeout);
		
		// throws FreewaveConnectionException if no 'CONNECT' answer
		wait4Connect(timeout);
	}
	
	/** 
	 * Sets the repeater path.
	 * @param repeaterEntry the repeater entry in the FreeWave phone book
	 */
	private void setRepeater(int repeater) throws Exception {
		String cmd = "ATXC" + (char)('0' + repeater);
		writeString(cmd);
		wait4OK(timeout);
	}
	
	/** 
	 * Waits for an OK from the FreeWave.
	 * @throws Exception if there was a problem
	 */
	private void wait4OK(int timeout) throws Exception {
		
		// throws SerialConnectionException if timeout
		String msg = readString(timeout);

		if (0 != msg.indexOf ("OK"))
			throw new Exception("'OK' expected but '" + msg + "' received");

		//sometimes "CONNECT" comes immediately after "OK", if so put the rest back to the message queue
		int idx = msg.indexOf("CONNECT");
		if (-1 != idx) {
			msgQueue.add(msg.substring(idx));
		}
	}
	
	/** 
	 * Waits for a CONNECT from the FreeWave.
	 * @param establishConnectionTimeout the timeout (ms) (-1 == none)
	 * @throws Exception if there was a problem
	 */
	private void wait4Connect (int timeout) throws Exception {
		
		// throws SerialConnectionException if timeout
		String msg = readString(timeout);

		if (0 != msg.indexOf ("CONNECT")) {
			throw new Exception ("'CONNECT' expected but '" + msg + "' received");
		}
	}
}