package gov.usgs.volcanoes.vdx.in.hw;

import java.util.Date;

import gov.usgs.util.ConfigFile;

/**
 * Interface to represent a device
 * 
 * @author Loren Antolik
 */
public interface Device {
	public void initialize (ConfigFile params) throws Exception;
	public String requestData (Date startDate) throws Exception;
	public boolean messageCompleted (String message) throws Exception;
	public boolean validateMessage (String message, boolean ignoreWrongAddress) throws Exception;
	public void validateLine (String line) throws Exception;
	public String formatMessage (String message);
	public String formatLine (String line);
	public String setTime ();
	public String getTimestamp();
	public String getTimezone();
	public String getDelimiter();
	public String getFields();
	public String getNullfield();
	public boolean getPollhist();
	public String toString();
	public int getTimeout();
	public int getMaxtries();
}
