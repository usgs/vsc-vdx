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
public class AGLILY implements Device {
	
	/** the minimum length of a message */
	protected final int MIN_MESSAGE_LENGTH = 40;
	
	/** the timestamp mask */
	protected String timestamp;
	
	/** the timezone */
	protected String timezone;
	
	/** the connection timout */
	protected int timeout;
	
	/** the maximum number of tries */
	protected int maxtries;
	
	/** the maximum number of lines to request */
	protected int maxlines;
	
	/** the current number of lines being requests */
	protected int currentlines;
	
	/** the sample rate of the device, seconds per acquisition */
	protected int samplerate;
	
	/** the delimeter of the data */
	protected String delimiter;
	
	/** the column to check for null in database */
	protected String nullfield;
	
	/** flag to set last data time to system default or NOW */
	protected boolean pollhist;
	
	/** the columns available on the device */
	protected String fields;
	
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
		timezone	= Util.stringToString(params.getString("timezone"), "GMT");
		timeout		= Util.stringToInt(params.getString("timeout"), 60000);
		maxtries	= Util.stringToInt(params.getString("maxtries"), 2);
		maxlines	= Util.stringToInt(params.getString("maxlines"), 30);
		samplerate	= Util.stringToInt(params.getString("samplerate"), 60);
		delimiter	= Util.stringToString(params.getString("delimiter"), ",");
		nullfield	= Util.stringToString(params.getString("nullfield"), "");
		pollhist	= Util.stringToBoolean(params.getString("pollhist"), true);
		fields		= Util.stringToString(params.getString("fields"), "");
		acquisition	= Acquisition.fromString(Util.stringToString(params.getString("acquisition"), "poll"));
		
		// validation
		if (fields.length() == 0) {
			throw new Exception("fields not defined");
		} else if (acquisition == null) {
			throw new Exception("invalid acquisition type");
		}
	}
	
	/**
	 * Get settings
	 */
	public String toString() {
		String settings	= "id:" + id + "/";
		settings	   += "acquisition:" + acquisition.toString() + "/";
		settings	   += "timestamp:" + timestamp + "/";
		settings	   += "timezone:" + timezone + "/";
		settings	   += "timeout:" + timeout + "/";
		settings	   += "maxtries:" + maxtries + "/";
		settings	   += "maxlines:" + maxlines + "/";
		settings	   += "samplerate:" + samplerate + "/";
		settings	   += "delimiter:" + delimiter + "/";
		settings	   += "nullfield:" + nullfield + "/";
		settings	   += "pollhist:" + pollhist + "/";
		return settings;
	}
	
	/**
	 * Request data
	 */
	public String requestData (Date startDate) throws Exception {
		
		String cmd = "";
		
		switch (acquisition) {

		case POLL: 
		
			// calculate the number of seconds since the last data request
			long secs	= (System.currentTimeMillis() - startDate.getTime()) / 1000;
			
			// calculate the number of samples since the last data request
			int samps	= (int) Math.floor(secs  / samplerate);
	
			// request the smaller of the two, samples accumulated or lines
			currentlines	= Math.min(samps, maxlines);
			
			// if no data is available then throw exception indicating we don't need to poll
			if (currentlines <= 0) {
				throw new Exception("no data to poll");
			} else {			
				cmd	+= "XY-DL-LAST," + currentlines;
			}
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
	    	} else if (!message.substring(length - 2).contentEquals("\r\n")) {
	    		return false;
	    	} else {
	    		return true;
	    	}

    	case POLL:
	    	if (length < MIN_MESSAGE_LENGTH) {
	    		return false;
	    	} else if (message.charAt(0) != '*') {
	    		return false;
	    	} else if (!message.substring(length - 15).contentEquals("$end download\r\n")) {
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
	    		throw new Exception ("Too short. Length = " + length + "\n" + message);
	    	} else if (message.charAt(0) != '$') {
	    		throw new Exception ("Wrong start character: " + message.charAt(0) + "\n" + message);
	    	} else if (!message.substring(length - 2).contentEquals("\r\n")) {
	    		throw new Exception ("Wrong end character: " + message.charAt(length - 2) + "\n" + message);
	    	}
    		break;
    		
    	case POLL:
	    	if (length < MIN_MESSAGE_LENGTH) {
	    		throw new Exception ("Too short. Length = " + length + "\n" + message);
	    	} else if (message.charAt(0) != '*') {
	    		throw new Exception ("Wrong start character: " + message.charAt(0) + "\n" + message);
	    	} else if (!message.substring(length - 15).contentEquals("$end download\r\n")) {
	    		throw new Exception ("Wrong end character: " + message.substring(length - 15) + "\n" + message);
	    	}
	    	break;
    	}
    	
    	return true;
    }
    
    public void validateLine (String line) throws Exception {    	
    	int length = line.length();   	
    	if (length < MIN_MESSAGE_LENGTH) {
    		throw new Exception("less than mininum message length");
    	} else if (line.charAt(0) != '$') {
    		throw new Exception("does not begin begin with $");
    	} else if (!line.substring(length - 1).contentEquals("\r")) {
    		throw new Exception("does not end with <CR>");
    	}
    }
    
    public String formatMessage (String message) {
    	
    	switch (acquisition) {

    	case POLL:
    		// remove the first line and the trailing message
    		message = message.substring(message.indexOf('\n') + 1, message.length() - 16);
    		break;
    	}
    	
    	return message;
    }
    
    /**
     * formats a lily data line.  removes the leading $ and the trailing \r
     */
    public String formatLine (String line) {
    	int length	= line.length();
    	line		= line.substring(1, length - 1);
    	return line.trim();
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
    public int getMaxtries() {
    	return maxtries;
    }
    
    /**
     * getter method for delimiter
     */
    public String getDelimiter() {
    	return delimiter;
    }
    
    /**
     * getter method for null fields
     */
    public String getNullfield() {
    	return nullfield;
    }
    
    /**
     * getter method for polling historical data
     */
    public boolean getPollhist() {
    	return pollhist;
    }
    
    /**
     * getter method for columns
     */
    public String getFields() {
    	return fields;
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
        Calendar rightNow = Calendar.getInstance (TimeZone.getTimeZone ("GMT"));
        SimpleDateFormat formatter = new SimpleDateFormat ("ss,mm,HH,dd,MM,yy");
        formatter.setTimeZone (TimeZone.getTimeZone (timezone));

        String cmd = "SET-TIME,";
        cmd += formatter.format (rightNow.getTime());
		return make(cmd);
	}
}