package gov.usgs.vdx.in.hw;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * A class that handles LILY tiltmeter commands
 * 
 * @author lantolik
 */
public class LilyDevice implements Device {
	
	/** the minimum length of a message */
	protected final int MIN_MESSAGE_LENGTH = 40;
	
	/** the timestamp mask */
	protected String timestamp;
	
	/** the timezone */
	protected String timezone;
	
	/** the connection timout */
	protected int timeout;
	
	/** the maximum number of tries */
	protected int tries;
	
	/** the maximum number of lines to request */
	protected int maxlines;
	
	/** the current number of lines being requests */
	protected int currentlines;
	
	/** the sample rate of the device, seconds per acquisition */
	protected int samplerate;
	
	/** the delimeter of the data */
	protected String delimiter;
	
	/** the columns available on the device */
	protected String columns;
	
	/** the acquisition mode */
	protected Acquisition acquisition;
	
	/** the id of the station */
	protected String id;
	
	private enum Acquisition {
		STREAM, POLL;
		public static Acquisition fromString(String s) {
			if (s.equalsIgnoreCase("stream")) {
				return STREAM;
			} else if (s.equals("poll")) {
				return POLL;
			} else {
				return null;
			}
		}
	}
	
	/**
	 * Initialize Lily Device
	 */
	public void initialize(ConfigFile params) throws Exception {
		id			= Util.stringToString(params.getString("id"), "0");
		timestamp	= Util.stringToString(params.getString("timestamp"), "MM/dd/yy HH:mm:ss");
		timezone	= Util.stringToString(params.getString("timezone"), "UTC");
		timeout		= Util.stringToInt(params.getString("timeout"), 30000);
		tries		= Util.stringToInt(params.getString("tries"), 2);
		maxlines	= Util.stringToInt(params.getString("maxlines"), 30);
		samplerate	= Util.stringToInt(params.getString("samplerate"), 60);
		delimiter	= Util.stringToString(params.getString("delimiter"), ",");
		columns		= Util.stringToString(params.getString("columns"), "");
		acquisition	= Acquisition.fromString(Util.stringToString(params.getString("acquisition"), "poll"));
		if (acquisition == null) {
			throw new Exception("LilyDevice initialize failed.  invalid acquisition");
		}
	}
	
	/**
	 * Get settings
	 */
	public String toString() {
		String settings	= "id:" + id + "/";
		settings	   += "timestamp:" + timestamp + "/";
		settings	   += "timezone:" + timezone + "/";
		settings	   += "timeout:" + timeout + "/";
		settings	   += "tries:" + tries + "/";
		settings	   += "maxlines:" + maxlines + "/";
		settings	   += "samplerate:" + samplerate + "/";
		settings	   += "delimiter:" + delimiter + "/";
		settings	   += "columns:" + columns + "/";
		settings	   += "acquisition:" + acquisition.toString() + "/";
		return settings;
	}
	
	/**
	 * Request data
	 */
	public String requestData (Date startDate) throws Exception {
		
		String cmd = "";
		
		switch (acquisition) {
		
		// stream mode doesn't require a command
		case STREAM:
			break;

		case POLL: 
		
			// calculate the number of seconds since the last data request
			long secs	= (System.currentTimeMillis() - startDate.getTime()) / 1000;
			
			// calculate the number of samples since the last data request
			int samps	= (int) Math.floor(secs  / samplerate);
	
			// request the smaller of the two, samples accumulated or lines
			currentlines	= Math.min(samps, maxlines);
			
			cmd	+= "XY-DL-LAST," + currentlines;
			break;			
		}
        
		return make(cmd);
	}
    
	/**
	 * Check if message is complete
	 */
    public boolean messageCompleted (String message) {
    	
    	int length = message.length();
    	
    	switch (acquisition) {
    	
    	case STREAM:
	    	if (length < MIN_MESSAGE_LENGTH) {
	    		return false;
	    	} else if (message.charAt(0) != '$') {
	    		return false;
	    	} else if (message.charAt(length - 2) != '\r') {
	    		return false;
	    	} else if (message.charAt(length - 1) != '\n') {
	    		return false;
	    	} else {
	    		return true;
	    	}

    	case POLL:
	    	if (length < MIN_MESSAGE_LENGTH) {
	    		return false;
	    	} else if (message.charAt(0) != '*') {
	    		return false;
	    	} else if (message.substring(length - 15, length - 2) != "$end download") {
	    		return false;
	    	} else if (message.charAt(length - 2) != '\r') {
	    		return false;
	    	} else if (message.charAt(length - 1) != '\n') {
	    		return false;
	    	} else {
	    		return true;
	    	}
    	}    	
    	return false;
    }
    
    public boolean validateMessage (String message, boolean ignoreWrongAddress) throws Exception {
    	
    	int length	= message.length();
    	
    	switch (acquisition) {
    	
    	case STREAM:
	    	if (length < MIN_MESSAGE_LENGTH) {
	    		throw new Exception ("Message invalid. Too short. Length = " + length);
	    	} else if (message.charAt(0) != '$') {
	    		throw new Exception ("Message invalid. Wrong start character: " + message.charAt(0));
	    	} else if (message.charAt(length - 2) != '\r') {
	    		throw new Exception ("Message invalid. Wrong <CR> character: " + message.charAt(length - 2));
	    	} else if (message.charAt(length - 1) != '\n') {
	    		throw new Exception ("Message invalid. Wrong <LF> character: " + message.charAt(length - 1));
	    	}
    		break;
    		
    	case POLL:
	    	if (length < MIN_MESSAGE_LENGTH) {
	    		throw new Exception ("Message invalid. Too short. Length = " + length);
	    	} else if (message.charAt(0) != '*') {
	    		throw new Exception ("Message invalid. Wrong start character: " + message.charAt(0));
	    	} else if (message.substring(length - 15, length - 2) != "$end download") {
	    		throw new Exception ("Message invalid. Wrong end download message: " + message.substring(length - 15, length - 2));
	    	} else if (message.charAt(length - 2) != '\r') {
	    		throw new Exception ("Message invalid. Wrong <CR> character: " + message.charAt(length - 2));
	    	} else if (message.charAt(length - 1) != '\n') {
	    		throw new Exception ("Message invalid. Wrong <LF> character: " + message.charAt(length - 1));
	    	}
	    	break;
    	}
    	
    	return true;
    }
    
    public boolean validateLine (String line) throws Exception {
    	
    	int length = line.length();
    	
    	if (length < MIN_MESSAGE_LENGTH) {
    		return false;
    	} else if (line.charAt(0) != '$') {
    		return false;
    	} else if (line.charAt(length - 2) != '\r') {
    		return false;
    	} else if (line.charAt(length - 1) != '\n') {
    		return false;
    	} else {
    		return true;
    	}
    }
    
    public String formatMessage (String message) {
    	
    	switch (acquisition) {
    	
    	case STREAM:
    		break;

    	case POLL:
    		message = message.substring(0, message.length() - 14);
    		break;
    	}
    	
    	return message;
    }
    
    /**
     * formats a lily data line.  removes the leading $ and the trailing \r
     */
    public String formatLine (String line) {
    	return line.substring(1, line.length() - 1).trim();
    }
    
    /**
     * getter method for timestamp mask
     */
    public String getTimestamp() {
    	return timestamp;
    }
    
    /**
     * getter method for data timeout
     */
    public String getTimezone() {
    	return timezone;
    }
    
    /**
     * getter method for data timeout
     */
    public int getTimeout() {
    	return timeout;
    }
    
    /**
     * getter method for tries
     */
    public int getTries() {
    	return tries;
    }
    
    /**
     * getter method for delimiter
     */
    public String getDelimiter() {
    	return delimiter;
    }
    
    /**
     * getter method for columns
     */
    public String getColumns() {
    	return columns;
    }
    
    /**
     * Generates a complete lily request string.
     * Adds the command prefix
     * 
     * @param msg the message string
     * 
     * @return the complete lily string
     */
    public String make (String msg) {
    	String completeStr	= "";
    	if (msg.length() > 0) {
	    	completeStr += "*9900";
	    	completeStr += msg;
	    	completeStr += (char)'\r';
	    	completeStr += (char)'\n';
    	}
    	return completeStr;
    }
	
	public String setTime () {
        Calendar rightNow = Calendar.getInstance (TimeZone.getTimeZone ("UTC"));
        SimpleDateFormat formater = new SimpleDateFormat ("ss,mm,HH,dd,MM,yy");
        formater.setTimeZone (TimeZone.getTimeZone ("UTC"));

        String cmd = "SET-TIME,";
        cmd += formater.format (rightNow.getTime());
		return make(cmd);
	}
}