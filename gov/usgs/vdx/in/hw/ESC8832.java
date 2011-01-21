package gov.usgs.vdx.in.hw;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.StringTokenizer;
import java.util.TimeZone;

/**
 * A class that handles ESC8832 data logger commands
 * 
 * @author Loren Antolik
 */
public class ESC8832 implements Device {
	
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
	
	/** the columns available on the device */
	protected String fields;
	
	/** the acquisition mode */
	protected Acquisition acquisition;
	
	/** the id of the station */
	protected String id;
	
	protected SimpleDateFormat dateIn	= new SimpleDateFormat("jjjHHmmss");
	protected SimpleDateFormat dateOut	= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
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
		timestamp	= Util.stringToString(params.getString("timestamp"), "yyyy-MM-dd HH:mm:ss");
		timezone	= Util.stringToString(params.getString("timezone"), "GMT");
		timeout		= Util.stringToInt(params.getString("timeout"), 60000);
		maxtries	= Util.stringToInt(params.getString("maxtries"), 2);
		maxlines	= Util.stringToInt(params.getString("maxlines"), 30);
		samplerate	= Util.stringToInt(params.getString("samplerate"), 60);
		delimiter	= Util.stringToString(params.getString("delimiter"), ",");
		nullfield	= Util.stringToString(params.getString("nullfield"), "");
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
			currentlines	= samps;
			
			// if no data is available then throw exception indicating we don't need to poll
			if (currentlines == 0) {
				throw new Exception("no data to poll");
			}
			
			// calculate the start date and end date
			Date endDate 	= new Date();
			// Calendar cal	= Calendar.getInstance();
			// cal.setTime(endDate);
			// long endSeconds	= cal.getTimeInMillis();
			// endSeconds -= endSeconds % (1000 * 60 * 15);			
			
			// build up the command else {
			cmd += "!5600015M";
			cmd += dateIn.format(startDate);
			cmd += "|Y|";
			cmd += dateIn.format(endDate);
			cmd	+= "&";
			
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

    	case POLL:
	    	if (length < MIN_MESSAGE_LENGTH) {
	    		return false;
	    	} else if (message.charAt(0) != '@') {
	    		return false;
	    	} else if (message.charAt(length - 1) != '$') {
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
    		
    	case POLL:
	    	if (length < MIN_MESSAGE_LENGTH) {
	    		throw new Exception ("Too short. Length = " + length + "\n" + message);
	    	} else if (message.charAt(0) != '@') {
	    		throw new Exception ("Wrong start character: " + message.charAt(0) + "\n" + message);
	    	} else if (message.charAt(length - 1) != '$') {
	    		throw new Exception ("Wrong end character: " + message.charAt(length - 1) + "\n" + message);
	    	}
	    	break;
    	}
    	
    	return true;
    }
    
    public void validateLine (String line) throws Exception {    	
    	int length = line.length();   	
    	if (length < MIN_MESSAGE_LENGTH) {
    		throw new Exception("less than mininum message length");
    	}
    }
    
    public String formatMessage (String message) {
    	
    	StringTokenizer st;
    	String line;
    	String dataChannel;
    	Date dataDate;
    	Double dataValue;
    	String dataFlag;
    	String dataDay;
    	ESC8832DataPacket dataPacket;
    	ArrayList<ESC8832DataPacket> dataPacketList;
    	
    	Date currentDate	= new Date();
    	String currentDay	= new SimpleDateFormat("DDD").format(currentDate);
    	String currentYear	= new SimpleDateFormat("yyyy").format(currentDate);
    	Calendar calendar	= new GregorianCalendar(TimeZone.getTimeZone("GMT"));
    	calendar.setTime(currentDate);
    	
    	switch (acquisition) {

    	case POLL:
    		
    		// trim the special characters at the beginning/end of the message
    		message = message.substring(4, message.length() - 15);
    		
    		// parse the message into individual time/data units
    		st				= new StringTokenizer(message, "!");
    		dataPacketList	= new ArrayList<ESC8832DataPacket>();
			while (st.hasMoreTokens()) {
				
				// get the next line and validate
				line		= st.nextToken();
				if (!line.startsWith("56") || !line.endsWith("&")) {
					continue;
				}
				
				// parse the channel 
				dataChannel	= line.substring(2, 4);
				
				// parse the date
				try {
					
					// define a date, but it has no year, so it will be 1970
					dataDate	= dateIn.parse(line.substring(8, 17));
					calendar.setTime(dataDate);
					
					// if the data jday is greater than the current jday, then the year of the data needs to be decremented
					dataDay		= line.substring(8, 11);					
					if (Integer.valueOf(dataDay) > Integer.valueOf(currentDay)) {
						calendar.set(Calendar.YEAR, Integer.valueOf(currentYear) - 1);
					} else {
						calendar.set(Calendar.YEAR, Integer.valueOf(currentYear));
					}
					
					// add the averaging interval to the date
					calendar.add(Calendar.SECOND, samplerate);
					
					// re-defined the data date
					dataDate	= calendar.getTime();						
						
				} catch (Exception e) {
					continue;
				}
				
				// parse the value
				try {
					dataValue	= Double.valueOf(line.substring(16, 26));
				} catch (Exception e) {
					continue;
				}
				
				// parse the flags
				if (line.indexOf("&") > 26) {
					dataFlag	= line.substring(26, line.length() - 1);
				} else {
					dataFlag	= "";
				}
				
				// apply the flags
				if (dataFlag.indexOf(">") >= 0) {
					dataValue	= Double.NaN;
				} else if (dataFlag.indexOf("C") >= 0) {
					dataValue	= Double.NaN;
				}
				
				// build the data packet and store it in a list
				dataPacket	= new ESC8832DataPacket(dataChannel, dataDate, dataValue);
				dataPacketList.add(dataPacket);
			}
			
			// if there are data packets then convert it to a comma separated message
			if (dataPacketList.size() > 0) {
    		
	    		// sort the parts, by date then by channel
				Object dataPacketArray[]	= dataPacketList.toArray();
				Arrays.sort(dataPacketArray, new ESC8832DataPacketComparator());
	    		
	    		// get the first data packet to start the output
				dataPacket	= (ESC8832DataPacket)dataPacketArray[0];
				currentDate	= dataPacket.dataDate;
				message		= dateOut.format(currentDate);
				
				// parse each data packet in the list
				for (int i = 0; i < dataPacketArray.length; i++) {
					dataPacket	= (ESC8832DataPacket)dataPacketArray[i];
					dataDate	= dataPacket.dataDate;
					dataValue	= dataPacket.dataValue;
					
					// if this is a new data date then create a new line
					if (dataDate != currentDate) {
						currentDate	= dataDate;
						message += "\n";
						message += dateOut.format(dataDate);
					}
					
					// output this info to the
					message += "," + dataValue;
				}
				
			} else {
				message = "";
			}

    		break;
    	}
    	
    	return message;
    }
    
    /**
     * formats a lily data line.  removes the leading $ and the trailing \r
     */
    public String formatLine (String line) {
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
	    	completeStr += "@HV";
	    	completeStr += msg;
	    	completeStr += "$";
    	}
    	return completeStr;
    }
	
	public String setTime () {
        String cmd = "";
		return make(cmd);
	}
}

/**
 * ESC8832DataPacket defines an object that contains a data channel, date and value
 * @author lantolik
 *
 */
class ESC8832DataPacket {	
	public String dataChannel;
	public Date dataDate;
	public Double dataValue;
	
	public ESC8832DataPacket (String dataChannel, Date dataDate, Double dataValue) {
		this.dataChannel	= dataChannel;
		this.dataDate		= dataDate;
		this.dataValue		= dataValue;
	}
}

/**
 * ESC8832DataPacketComparator compares two packets, first by date then by channel
 * @author lantolik
 *
 */
class ESC8832DataPacketComparator implements Comparator<Object> {
	public int compare(Object a, Object b) {
		int dateCompare	= ((ESC8832DataPacket)a).dataDate.compareTo(((ESC8832DataPacket)b).dataDate);
		if (dateCompare != 0) {
			return dateCompare;
		} else {
			return ((ESC8832DataPacket)a).dataChannel.compareTo(((ESC8832DataPacket)b).dataChannel);			
		}
	}	
}