package gov.usgs.vdx.in.hw;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;

import java.text.*;
import java.util.*;

/**
 * A class that handles <i>CCSAIL</i> commands.
 *
 * <i>CCSAIL</i> is a simple ASCII-based bi-directional command-
 * acknowledgement protocol used to set parameters or request
 * data from a <i>ZENO-3200</i> datalogger
 * from <a href="http://www.coastal.org"><i>Coastal Environmental Systems</i></a>
 * <p>
 * A CCSAIL command includes following parts:
 * <ul>
 * <li>ATN attention char ('#', dec 35, hex 23)</li>
 * <li>ADR address = ZENO unit ID (4 digits: 0001,...,9999)</li>
 * <li>RTN return address (always 0000 if the command is send from the master to a station)</li>
 * <li>MSG CCSAIL message</li>
 * <li>CHK Checksum (2 digit ASCII dezimal number)</li>
 * <li>ETX end-of-transmission character (hex 03)</li>
 * </ul>
 * <p>
 * History: <ul>
 * <li> 2000/11/24 0.10 first version</li>
 * <li> 2000/11/26 0.11 new: CCSAIL Command SDO for controling a digital output</li>
 * <li> 2001/01/13 1.00 first release</li>
 * <li> 2002/05/03 2.00 new: time set command</li>
 * <li> 2002/05/06      new: invalid command to test communication</li>
 * <li> 2002/05/06      changed: getMsg with parameter ignoreADR  </li>
 * </ul>
 *
 * @version 2.1
 * @author Ralf Krug, Loren Antolik
 *
 */
public class CCSAIL implements Device {
	
	/** the minimum length of a message */
	protected final int MIN_MESSAGE_LENGTH = 20;
	
	/** the timestamp mask */
	protected String timestamp;
	
	/** the time zone */
	protected String timezone;
	
	/** the connection timeout */
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
	 * Initialize CCSAIL Device
	 */
	public void initialize(ConfigFile params) throws Exception {
		id			= Util.stringToString(params.getString("id"), "0");
		timestamp	= Util.stringToString(params.getString("timestamp"), "yy/MM/dd HH:mm:ss");
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
		
		// do some additional formatting for the device id
		int idval;
		try {
			idval	= Integer.valueOf(id);
		} catch (NumberFormatException e) {
			throw new Exception ("invalid id");
		}
        if (idval > 0 && idval < 10000) {
            DecimalFormat stationNumberFormatter = new DecimalFormat ("0000");
            id = stationNumberFormatter.format(idval);
        } else {
            throw new Exception ("Invalid id");
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
     * Generates a CCSAIL command string for the <code>DA</code> command.
     * <i>Request nn data sets logged on or after the indicated start date-time value</i>
     *
     * @param startDateL the start date for the polled values as a <code>long</code>
     * @param samplesInRequest the number of date sets which are polled
     *
     * @exception Exception if <code>samplesInRequest</code> are not in range 1,...,9999
     * @return the complete CCSAIL command string
     */
    public String requestData (Date startDate) throws Exception {
		
		// set the current number of lines to the max lines value
		currentlines	= maxlines;
		
        if ((currentlines < 1) || (currentlines > 9999))
            throw new Exception ("DA: number of lines (" + currentlines + ") not in range 1,...,9999");

        SimpleDateFormat formatter = new SimpleDateFormat ("yyMMddHHmmss");
        formatter.setTimeZone (TimeZone.getTimeZone (timezone));
        // Date startDate = new Date (startDateL);
        String cmd = "DA";

        cmd += formatter.format(startDate);
        cmd += currentlines;
        cmd += ",";

        return make(cmd);
    }
    
	/**
	 * Check if message is complete
	 */
    public boolean messageCompleted (String message) throws Exception {
    	if (message.charAt(message.length() - 1) == (char)3) {
    		return true;
    	} else {
    		return false;
    	}
    }
    
    /**
     * parses a CCSAIL command, checks it's components and returns the MSG part.
     * The CCSAIL command can be preceded by an CR/LF
     *
     * @param str the CCSAIL command string which should be parsed
     * @param ignoreWrongAddress if true, a wrong ADR in the message is not considered as an error
     *        this happens e.g. for timeset, changeGain commands (not for get data commands)
     *
     * @return the message (MSG) part of the <code>str</code>
     *
     * @exception Exception if the string does not match the CCSAIL format,
     *                            if it has an incorrect checksum,
     *                            if it has an incorrect station number
     */
    public boolean validateMessage (String message, boolean ignoreWrongAddress) throws Exception {
        String  token;
        int     len = message.length();

        if (message.startsWith ("\r\n")) {
        	message = message.substring (2, len);
            len -= 2;
        }

        if (len < MIN_MESSAGE_LENGTH) {
            throw new Exception ("Too short. Len = " + len + "\n" + message);
		} else if (message.charAt(0) != '#') {
            throw new Exception ("Wrong ATN: " + message.charAt(0) + "\n" + message);
		} else if (message.charAt(len - 1) != (char)3) {
            throw new Exception ("Wrong ETX: " + message.charAt (len - 1) + "\n" + message);
		}

        int checkSumIs   = (message.charAt(len - 3) - '0') * 10;
            checkSumIs  += (message.charAt(len - 2) - '0');
        int checkSumCalc = calculateChecksum(message, true);
        if (checkSumIs != checkSumCalc) {
            throw new Exception ("Wrong checksum: " + checkSumIs + " should be: " + checkSumCalc + "\n" + message);
		}

        token = message.substring (1, 5);
        if (!token.equals ("0000") && !ignoreWrongAddress) {
            throw new Exception ("Wrong ADR: " + token + "\n" + message);
		}

        token = message.substring (5, 9);
        if (!token.equals (id) && !ignoreWrongAddress) {
            throw new Exception ("Wrong RTN: " + token + "\n" + message);
		}
        
        return true;
    }
    
    /**
     * Validates a single line from a device
     * @param line the line to validate
     * @return true if line is validated
     */
    public void validateLine(String line) throws Exception {
    	int length = line.length();
		if (length <= 1) {
			throw new Exception ("length is 0");
		} else if (line.startsWith("EOF")) {
    		throw new Exception ("begins with EOF");
    	} else if (line.startsWith("#")) {
    		throw new Exception ("begins with #");
    	}
    }
    
    /**
     * Formats a message.  Removes unnecessary characters
     */
    public String formatMessage (String message) {

        // sometimes there is a new line at the beginning of the message
        if (message.startsWith("\r\n")) {
        	message = message.substring (2, message.length());
        }
        
        // remove the ATN, trailing checksum and \r
        message	= message.substring(11, message.length() - 3);
        
        // sometimes there is an EOF on the message
        if (message.substring(message.length() - 5).contentEquals("\nEOF,")) {
        	message = message.substring(0, message.length() - 5);
        }

        return message;
    }
    
    /**
     * Formats a line.  Removes unnecessary characters
     */
    public String formatLine(String line) {
        int length = line.length();
    	if (line.charAt(length - 1) == '\r') {
    		line = line.substring(0, length - 1);
    	}
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
     * Generates a complete CCSAIL string.
     * Adds the header (ATN, ADR, RTN) and the trailer (CHK, ETX)
     *
     * @param msg the message string
     * @param stationNumber the station number to which the ccsail command will be send
     *
     * @return the complete CCSAIL string
     */
    public String make (String msg) {
    	String completeStr = "";
        completeStr += (char)'#';									// ATN

        completeStr += id;                              	 		// ADR
        completeStr += "0000";										// RTN
        completeStr += msg;											// MSG

        int checksum = calculateChecksum (completeStr, false);		// CHK
        completeStr += (char)('0' + (checksum / 10));
        completeStr += (char)('0' + (checksum % 10));

        completeStr += (char)3;										// ETX

        return completeStr;
    }

    /**
     * Generates a CCSAIL command string for the <code>DL</code> command.
     * <i>Request the latest nn data sets stored in the data logging memory</i>
     *
     * @param numberOfValues the number of data sets which are polled
     * @exception Exception if <code>numberOfValues</code> is not in range 1,...,9999
     * @return the complete CCSAIL command string
     */
    public String requestData (int numberOfValues) throws Exception {
        if ((numberOfValues < 1) || (numberOfValues > 9999))
            throw new Exception ("DL: number of values (" + numberOfValues + ") not in range 1,...,9999");

        String cmd = "DL";
        cmd += numberOfValues;
        cmd += ',';

        return (make(cmd));
    }

    /**
     * Generates a CCSAIL command string for the <code>DB</code> command.
     * <i>Request alldata sets logged between the indicated start date-time value
     * and the stop date-time</i>
     *
     * @param startDate the start date for the polled values
     * @param stopDate the stop date for the polled values
     *
     * @return the complete CCSAIL command string
     */
    public String requestData (Date startDate, Date stopDate) throws Exception {
        SimpleDateFormat formater = new SimpleDateFormat ("yyMMddHHmmss");
        formater.setTimeZone (TimeZone.getTimeZone (timezone));

        String cmd = "DB";
        cmd += formater.format (startDate);
        cmd += formater.format (stopDate);

        return (make(cmd));
    }

    /**
     * Generates a CCSAIL command string for the <code>TM</code> command.
     * <i>Addressed time set command</i>
     *
     * @return the complete CCSAIL command string
     */
    public String setTime () {
        Calendar rightNow = Calendar.getInstance (TimeZone.getTimeZone ("GMT"));
        SimpleDateFormat formater = new SimpleDateFormat ("yyMMddHHmmss");
        formater.setTimeZone (TimeZone.getTimeZone (timezone));

        String cmd = "TM";
        cmd += formater.format (rightNow.getTime());

        return (make(cmd));
    }

    /**
     * Generates an CCSAIL command string, which is unknown for the Zeno.
     * It is used to check the communication which the Zeno, because the
     * Zeno should return a NAK
     *
     * @return the complete CCSAIL command string
     */
    public String makeInvalid () {
        return (make ("TT"));
    }

    /**
     * Generates a CCSAIL command string for the <code>SDO</code> command.
     * <i>Sets the level of a digital output</i>
     *
     * @param on the level to which the digital output should be set
     * @param outputNumber The number of the digital output
     * @exception Exception if <code>outputNumber</code> is not in range 18,...,24
     * @return the complete CCSAIL command string
     */
    public String makeSDO (boolean on, int outputNumber) throws Exception {
        if ((outputNumber < 18) || (outputNumber > 24))
            throw new Exception ("SDO: output number (" + outputNumber + ") not in range 18,...,24");

        String cmd = "SDO";
        cmd += (on ? '1' : '0');
        cmd += outputNumber;

        return (make(cmd));
    }

    /**
     * Generates a CCSAIL command string to switch digital output Nr. 18 to a low or high level.
     * <i>The tiltmeter's gain wire, which runs to the gain switch, is connected to
     * Zeno's digital output No. 18 via a transistor.
     * High level on the digital output opens the transistor,
     * and the transistor will ground the gain wire, which results in "high" gain.</i>
     *
     * @param high if <code>true</code>, switch to high gain
     * @return the complete CCSAIL command string
     */
    public String makeChangeTiltmeterGain (boolean high) {
        String cmd = "";
        try {
            cmd = makeSDO (high, 18);
        } catch (Exception e) { } // should not happen

        return cmd;
    }


    /**
     * checks, if a ccsail command string has a correct checksum
     *
     * @param str the string to be checked (including ATN, CHK and ETX)
     *
     * @return <code>true</code> if the string matches the CCSAIL format and has the correct checksum
     */
    private boolean checkChecksum (String str) {
        boolean chkOk = false;

        int len = str.length();
        if (chkOk = (len >= 4)) {
            if (chkOk = (((char)3 == str.charAt(len - 1)) && ('#' == str.charAt (0)))) {
                int chkIs = (str.charAt (len - 3) - '0') * 10;
                chkIs += (str.charAt (len - 2) - '0');
                chkOk = (chkIs == calculateChecksum (str, true));
            }
        }

        return chkOk;
    }


    /**
     * calculates the checksum.
     * The checksum is computed by adding the ASCII values of all characters
     * in ADR, RTN and MSG. The checksum does not include the ATN oder ETX character.
     * The ASCII sum is divided by 100 and the remainder is the checksum.
     *
     * @param str the string including ATN
     * @param ignoreEnd if <code>true</code> the last three chars are ignored
     *                  (they contain CHK and ETX)
     *
     * @return the checksum as an integer
     */
    private int calculateChecksum (String str, boolean ignoreEnd) {
        int checksum = 0;
        int c = 0;
        int len = str.length();
        if (ignoreEnd) len -= 3;

        // exclude the '#' from calculating the checksum
        for (int i = 1; i < len; i++) {
            c = str.charAt (i);
            checksum += c;
            //System.out.println ("i=" + i + " c=" + c + " ChkDigit=" + checksum);
        }
        checksum %= 100;

        return checksum;
    }
}