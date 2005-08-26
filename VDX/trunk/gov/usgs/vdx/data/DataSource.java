package gov.usgs.vdx.data;

import gov.usgs.vdx.server.RequestResult;

import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public interface DataSource 
{
	public String getType();
	public void initialize(Map<String, Object> params);
	public RequestResult getData(Map<String, String> params);
}
