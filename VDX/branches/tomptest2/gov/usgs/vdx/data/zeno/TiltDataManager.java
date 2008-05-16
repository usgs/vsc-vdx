package gov.usgs.vdx.data.zeno;

/**
 * A TiltDataManager is the high-level abstraction of a data source that
 * provides tilt data.
 *
 * $Log: not supported by cvs2svn $
 *
 * @@author Dan Cervelli
 * @@version 2.00
 */
public interface TiltDataManager 
{
	/** Gets tilt data for a given time and station.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the tilt data
	 */
    public TiltData getTiltData(String station, double t1, double t2);
	
	/** Gets tilt data at the extreme edges of a given time interval and station.
	 * Used for optimization purposes in generating tilt vectors.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the tilt data
	 */
    public TiltData getTiltDataEdges(String station, double t1, double t2);
	
	/** Gets a list of tilt data stations.
	 * @@return a list of stations (element 0 is id, element 1 is name)
	 */
    public String[][] getStations();
	
	/** Gets the physical lon/lat location of a station.
	 * @@param station the station
	 * @@return double array of length 2, lon/lat
	 */
    public double[] getLocation(String station);
	
	/** Gets the nominal azimuth to the source of inflation/deflation.
	 * @@param station the station
	 * @@return the nominal azimuth in degrees
	 */
    public double getNominalAzimuth(String station);
	
	/** Outputs raw tilt data to a file.
	 * @@param filename output filename
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 */
    public void outputRawData(String filename, String station, double t1, double t2);
	
	/** Gets tilt environment data for a given time and station.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the tilt environment data
	 */
    public TiltEnvData getTiltEnvData(String station, double t1, double t2);
	
	/** Outputs raw tilt environment data to a file.
	 * @@param filename output filename
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 */
    public void outputRawEnvData(String filename, String station, double t1, double t2);
}