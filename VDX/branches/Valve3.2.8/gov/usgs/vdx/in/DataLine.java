package gov.usgs.vdx.in;

import java.util.List;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import gov.usgs.util.Util;

public class DataLine {
	
	// instantiation variables
	public List<DataType> dataTypeList;
	public String delimiter;
	public String dataLine;
	
	// parsing variables
	public DataType dataType;
	public StringTokenizer keys;
	public StringTokenizer values;
	public String key;
	public String value;
	
	// date formatting variables
	public String dateMask;
	public String dateString;
	public SimpleDateFormat simpleDateFormat;
	public Date date;
	
	// data variables
	public Double t, x, y, h, b, i, g, r, s1, s2, bar, co2l, co2h;
	public String sid;
	
	/**
	 * Default constructor
	 */
	public DataLine(){}
	
	/**
	 * User defined constructor.  Takes the dataTypeList defined in the config file, the delimiter
	 * defined in the config file, and a dataLine from the instrument, and parses it into the available 
	 * variables. For the time being this system only supports tilt, strain and gas data types.
	 * 
	 * @param dataTypeList
	 * @param delimiter
	 * @param dataLine
	 */
	public DataLine (List<DataType> dataTypeList, String delimiter, String dataLine) {
		
		// assign the variables locally
		this.dataTypeList	= dataTypeList;
		this.delimiter		= delimiter;
		this.dataLine		= dataLine;
		dateMask			= "";
		dateString			= "";
		x					= Double.NaN;
		y					= Double.NaN;
		h					= Double.NaN;
		b					= Double.NaN;
		i					= Double.NaN;
		g					= Double.NaN;
		r					= Double.NaN;
		s1					= Double.NaN;
		s2					= Double.NaN;
		bar					= Double.NaN;
		co2l				= Double.NaN;
		co2h				= Double.NaN;
		
		// create an iterator to parse through this line
		values = new StringTokenizer(dataLine, delimiter);
		
		// parse through the dataTypeList and assign the values from the dataLine
		for (int j = 0; j < dataTypeList.size(); j++) {
			
			// get the current dataType that we are working with
			dataType = dataTypeList.get(j);
			
			// verify that the list item is in the proper order
			if (j != dataType.order) {
				System.out.println(dataType.order + " out of order...");
			}
			
			// create an iterator to parse through these data types
			keys = new StringTokenizer(dataType.format, ",");
			
			// iterate over the elements and parse into data structures(sid, yy/MM/dd, HH:mm:ss, i, b, x, y, h, g, r)
			while (keys.hasMoreTokens()) {
				
				// get the key and value pair for this piece of data
				key		= keys.nextToken();
				value	= values.nextToken();
				
				// timestamp data types
				if (dataType.type.equals("timestamp")) {
					dateMask	= dateMask + key + " ";
					dateString	= dateString + value + " ";
					
				// individual data types
				} else if (dataType.type.equals("data")) {
					if (key.equals("sid")) {
						sid		= value;
					} else if (key.equals("x")) {
						x		= Double.parseDouble(value);
					} else if (key.equals("y")) {
						y		= Double.parseDouble(value);
					} else if (key.equals("h")) {
						h		= Double.parseDouble(value);
					} else if (key.equals("b")) {
						b		= Double.parseDouble(value);
					} else if (key.equals("i")) {
						i		= Double.parseDouble(value);
					} else if (key.equals("g")) {
						g		= Double.parseDouble(value);
					} else if (key.equals("r")) {
						r		= Double.parseDouble(value);
					} else if (key.equals("s1")) {
						s1		= Double.parseDouble(value);
					} else if (key.equals("s2")) {
						s2		= Double.parseDouble(value);
					} else if (key.equals("bar")) {
						bar		= Double.parseDouble(value);
					} else if (key.equals("co2l")) {
						co2l	= Double.parseDouble(value);
					} else if (key.equals("co2h")) {
						co2h	= Double.parseDouble(value);
					}
					
				// ignorable data types
				} else if (dataType.type.equals("ignore")) {					
				}
			}
		}
		
		// parse the timestamp into a java date and convert to j2ksec
		simpleDateFormat = new SimpleDateFormat(dateMask);
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		try {
			date	= simpleDateFormat.parse(dateString);
			t		= Util.dateToJ2K(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}		
	}
}