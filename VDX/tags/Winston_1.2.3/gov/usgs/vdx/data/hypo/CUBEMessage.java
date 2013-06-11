package gov.usgs.vdx.data.hypo;

import gov.usgs.util.Log;
import gov.usgs.util.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Logger;

/**
 * 
 * Message structure:
 * 
 *TpEidnumbrSoVYearMoDyHrMnSecLatddddLongddddDeptMgNstNphDminRmssErhoErzzGpMNmEmLC
 *12345678901234567890123456789012345678901234567890123456789012345678901234567890
 *         1         2         3         4         5         6         7         8
 *
 *a2 * Tp   = Message type = "E " (seismic event)
 *a8 * Eid  = Event identification number  (any string)
 *a2 * So   = Data Source =  regional network designation
 *a1 * V    = Event Version     (ASCII char, except [,])
 *i4 * Year = Calendar year                (GMT) (-999-6070)
 *i2 * Mo   = Month of the year            (GMT) (1-12)
 *i2 * Dy   = Day of the month             (GMT) (1-31)
 *i2 * Hr   = Hours since midnight         (GMT) (0-23)
 *i2 * Mn   = Minutes past the hour        (GMT) (0-59)
 *i3 * Sec  = Seconds past the minute * 10 (GMT) (0-599)
 *i7 * Lat  = Latitude:  signed decimal degrees*10000 north>0
 *i8 * Long = Longitude: signed decimal degrees*10000 west <0
 *i4   Dept = Depth below sea level, kilometers * 10
 *i2   Mg   = Magnitude * 10
 *i3   Nst  = Number of stations used for location
 *i3   Nph  = Number of phases used for location
 *i4   Dmin = Distance to 1st station;   kilometers * 10
 *i4   Rmss = Rms time error; sec * 100
 *i4   Erho = Horizontal standard error; kilometers * 10
 *i4   Erzz = Vertical standard error;   kilometers * 10
 *i2   Gp   = Azimuthal gap, percent of circle; degrees/3.6
 *a1   M    = Magnitude type
 *i2   Nm   = Number of stations for magnitude determination
 *i2   Em   = Standard error of the magnitude * 10
 *a1   L    = Location method
 *a1 * C    = Menlo Park check character, defined below
 * 
 * 
 * TODO: modernize (use Time, Enum, etc.)
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class CUBEMessage
{
	protected final static Logger logger = Log.getLogger("gov.usgs.vdx.data.hypo.CUBEMessage"); 
    private static SimpleDateFormat dateIn = new SimpleDateFormat("yyyyMMddHHmmss");
    private static SimpleDateFormat dateOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    public static final int EARTHQUAKE = 1;
    public static final int DELETE_EARTHQUAKE = 2;
    public static final int TEXT_MESSAGE = 3;
    public static final int LINK = 4;
    public static final int UNKNOWN = 5;
    private String message;
    
  
    /**
     * Constructor
     * @param m message string
     */
    public CUBEMessage(String m)
    {
        message = m;
    }
    
    /**
     * Get message type
     * @return message type
     */
    public int getMessageType()
    {
        if (message == null || message.length() == 0)
            return UNKNOWN;
        String mt = message.substring(0, 2);
        if (mt.equals("E "))
            return EARTHQUAKE;
        else if (mt.equals("DE"))
            return DELETE_EARTHQUAKE;
        else if (mt.equals("TX"))
            return TEXT_MESSAGE;
        else if (mt.equals("LI"))
            return LINK;
        else
            return UNKNOWN;
    }
    
    /**
     * Get event ID
     * @return event id
     */
    public String getEventID()
    {
        return message.substring(2, 10);
    }
    
    /**
     * Get data source
     * @return data source
     */
    public String getDataSource()
    {
        return message.substring(10, 12);
    }
    
    /**
     * Get message version
     * @return version
     */
    public char getVersion()
    {
        return message.charAt(12);
    }
    
    /**
     * Get message date
     * @return date as Date
     */
    public Date getDate()
    {
        try
        {
            String ds = message.substring(13, 27);
            Date d = dateIn.parse(ds);
            int ts = Integer.parseInt(message.substring(27, 28));
            d.setTime(d.getTime() + ts * 100);
            return d;
        } 
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return new Date(0);
    }
  
    /**
     * Get message date as j2k
     * @return date as j2k
     */
    public double getJ2KSec()
    {
        return Util.dateToJ2K(getDate());
    }
    
    /**
     * Get message event latitude
     * @return latitude
     */
    public double getLatitude()
    {
        String lat = message.substring(28, 35).trim();
        double r = 0.0;
        try { r = Double.parseDouble(lat) / 10000; } catch (Exception e) {}
        return r;
    }
 
    /**
     * Get message event longitude
     * @return longitude
     */
    public double getLongitude()
    {
        String lon = message.substring(35, 43).trim();
        double r = 0.0;
        try { r = Double.parseDouble(lon) / 10000; } catch (Exception e) {}
        return r;
    }
    
    /**
     * Get message event depth
     * @return depth
     */
    public double getDepth()
    {
        String depth = message.substring(43, 47).trim();
        double r = 0.0;
        try {  r = Double.parseDouble(depth) / 10; } catch (Exception e) {}
        return r;
    }
   
    /**
     * Get message event magnitude
     * @return magnitude
     */
    public double getMagnitude()
    {
        String depth = message.substring(47, 49).trim();
        double r = 0.0;
        try { r = Double.parseDouble(depth) / 10; } catch (Exception e) {}
        return r;
    }
    
    /**
     * Main method
     * Reads all files in the current directory, assume each of them contains one message.
     * Print all found earthquakes event on stdout.
     * @param args command line arguments
     */
    public static void main(String[] args)
    {
        try
        {
            File[] files = new File(".").listFiles();
            for (int i = 0; i < files.length; i++)
            {
                BufferedReader in = new BufferedReader(new FileReader(files[i]));
                CUBEMessage msg = new CUBEMessage(in.readLine());
                if (msg.getMessageType() == EARTHQUAKE)
                   logger.info(files[i].getName() + ": " + msg.getEventID() + " " + msg.getVersion() + " " + msg.getDataSource() + " " + dateOut.format(msg.getDate()) + " " + msg.getLongitude() + 
                        " " + msg.getLatitude() + " " + msg.getDepth() + " " + msg.getMagnitude());
                in.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        
    }
    
    static
    {
        dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
        dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
}
