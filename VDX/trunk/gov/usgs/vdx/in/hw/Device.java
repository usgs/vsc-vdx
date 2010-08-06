package gov.usgs.vdx.in.hw;

import java.util.Date;

/**
 * Interface to represent a device
 * 
 * @author Loren Antolik
 */
public interface Device {
	
	public String validateMessage (String message, boolean ignoreWrongAddress) throws Exception;
	public String requestData (Date startDate, Date endDate) throws Exception;
	public String requestData (Date startDate, int numberOfLines) throws Exception;
	public String requestData (int numberOfLines) throws Exception;
	public String setTime ();
}