package gov.usgs.vdx.in.hw;

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
 * @version 2.00
 * @author Ralf Krug
 *
 */
class CCSAILMessage
{
    private String stationNrString;

    /**
     * creates a CCSAIL object for a certain station
     *
     * @param stationNumber the station number to which the ccsail command will be send
     *                      or from which the ccsail command is expected.
     * @exception Exception if station number is not in range (1,...,9999).
     */
    public CCSAILMessage (int stationNumber) throws Exception
    {
        if ( (stationNumber > 0) && (stationNumber < 10000) )
        {
            DecimalFormat ccsailStationNrFormatter = new DecimalFormat ("0000");
            stationNrString = ccsailStationNrFormatter.format (stationNumber);
        }
        else
            throw new Exception ("Invalid station number");
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
    public String make (String msg)
    {
        String completeStr = "#";                                        // ATN

        completeStr += stationNrString;                                  // ADR
        completeStr += "0000";                                           // RTN
        completeStr += msg;                                              // MSG

        int checksum = calculateChecksum (completeStr, false);           // CHK
        completeStr += (char)('0' + (checksum / 10));
        completeStr += (char)('0' + (checksum % 10));

        completeStr += (char)3;                                          // ETX


        return completeStr;
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
    public String makeDA (Date startDate, int samplesInRequest) throws Exception
    {
        String cmd = "DA";

        if ((samplesInRequest < 1) || (samplesInRequest > 9999))
            throw new Exception ("DA: number of values (" + samplesInRequest + ") not in range 1,...,9999");

        SimpleDateFormat formater = new SimpleDateFormat ("yyMMddHHmmss");
        formater.setTimeZone (TimeZone.getTimeZone ("UTC"));
//        Date startDate = new Date (startDateL);

        cmd += formater.format (startDate);
        cmd += samplesInRequest;
        cmd += ",";

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
    public String makeDB (Date startDate, Date stopDate)
    {
        String cmd = "DB";

        SimpleDateFormat formater = new SimpleDateFormat ("yyMMddHHmmss");
        formater.setTimeZone (TimeZone.getTimeZone ("UTC"));

        cmd += formater.format (startDate);
        cmd += formater.format (stopDate);

        return (make(cmd));
    }

    /**
     * Generates a CCSAIL command string for the <code>DL</code> command.
     * <i>Request the latest nn data sets stored in the data logging memory</i>
     *
     * @param numberOfValues the number of data sets which are polled
     * @exception Exception if <code>numberOfValues</code> is not in range 1,...,9999
     * @return the complete CCSAIL command string
     */
    public String makeDL (int numberOfValues) throws Exception
    {
        if ((numberOfValues < 1) || (numberOfValues > 9999))
            throw new Exception ("DL: number of values (" + numberOfValues + ") not in range 1,...,9999");

        String cmd = "DL";
        cmd += numberOfValues;
        cmd += ',';

        return (make(cmd));
    }

    /**
     * Generates a CCSAIL command string for the <code>TM</code> command.
     * <i>Addressed time set command</i>
     *
     * @return the complete CCSAIL command string
     */
    public String makeTM ()
    {
        Calendar rightNow = Calendar.getInstance (TimeZone.getTimeZone ("UTC"));
        SimpleDateFormat formater = new SimpleDateFormat ("yyMMddHHmmss");
        formater.setTimeZone (TimeZone.getTimeZone ("UTC"));

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
    public String makeInvalid ()
    {
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
    public String makeSDO (boolean on, int outputNumber) throws Exception
    {
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
    public String makeChangeTiltmeterGain (boolean high)
    {
        String cmd = "";
        try
        {
            cmd = makeSDO (high, 18);
        }
        catch (Exception ccex) { } // should not happen

        return cmd;
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
    public String getMsg (String str, boolean ignoreWrongAddress) throws Exception
    {
        String  token;
        int     len = str.length();

        if (str.startsWith ("\r\n")) {
            str = str.substring (2, len);
            len -= 2;
        }

        if (len < 12) {
            throw new Exception ("Too short. Len = " + len);
		}

        if ('#' != str.charAt (0)) {
            throw new Exception ("Wrong ATN: " + str.charAt(0));
		}

        if ((char)3 != str.charAt (len - 1)) {
            throw new Exception ("Wrong ETX: " + str.charAt (len - 1));
		}

        int checkSumIs   = (str.charAt (len - 3) - '0') * 10;
            checkSumIs  += (str.charAt (len - 2) - '0');
        int checkSumCalc = calculateChecksum (str, true);
        if (checkSumIs != checkSumCalc) {
            throw new Exception ("Wrong checksum: " + checkSumIs + " should be: " + checkSumCalc);
		}

        token = str.substring (1, 5);
        if (!token.equals ("0000") && !ignoreWrongAddress) {
            throw new Exception ("Wrong ADR: " + token);
		}

        token = str.substring (5, 9);
        if (!token.equals (stationNrString) && !ignoreWrongAddress) {
            throw new Exception ("Wrong RTN: " + token);
		}

        // get the message
        token = str.substring (9, len - 3);
        return token;
    }


    /**
     * checks, if a ccsail command string has a correct checksum
     *
     * @param str the string to be checked (including ATN, CHK and ETX)
     *
     * @return <code>true</code> if the string matches the CCSAIL format and has the correct checksum
     */
    private boolean checkChecksum (String str)
    {
        boolean chkOk = false;

        int len = str.length();
        if (chkOk = (len >= 4))
        {
            if (chkOk = (   ((char)3 == str.charAt (len - 1))
                         && ('#'     == str.charAt (0)      ) ) )
            {
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
    private int calculateChecksum (String str, boolean ignoreEnd)
    {
        int checksum = 0;
        int c = 0;
        int len = str.length();
        if (ignoreEnd) len -= 3;

        for (int i = 1; i < len; i++) // exclude the '#' from calculating the checksum
        {
            c = str.charAt (i);
            checksum += c;
            //System.out.println ("i=" + i + " c=" + c + " ChkDigit=" + checksum);
        }
        checksum %= 100;

        return checksum;
    }

}
