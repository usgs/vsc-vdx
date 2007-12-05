package gov.usgs.vdx.data.zeno;

/**
 * A StrainDataManager is the high-level abstraction of a data source that
 * provides strain data.
 *
 * $Log: not supported by cvs2svn $
 *
 * @@author Dan Cervelli
 * @@version 2.00
 */
public interface StrainDataManager
{
	/** Gets strain data for a given time and station.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the strain data
	 */
    public StrainData getStrainData(String station, double t1, double t2);
	
	/** Gets a list of strain data stations.
	 * @@return a list of stations (element 0 is id, element 1 is name)
	 */
    public String[][] getStations();
	
	/** Gets the physical lon/lat location of a station.
	 * @@param station the station
	 * @@return double array of length 2, lon/lat
	 */
    public double[] getLocation(String station);
	
	/** Outputs raw strain data to a file.
	 * @@param filename output filename
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 */
    public void outputRawData(String filename, String station, double t1, double t2);
	
	/** Gets strain environment data for a given time and station.
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 * @@return the strain environment data
	 */
    public StrainEnvData getStrainEnvData(String station, double t1, double t2);
	
	/** Outputs raw strain environment data to a file.
	 * @@param filename output filename
	 * @@param station the station
	 * @@param t1 the start time
	 * @@param t2 the end time
	 */
    public void outputRawEnvData(String filename, String station, double t1, double t2);
}