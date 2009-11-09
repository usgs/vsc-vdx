package gov.usgs.vdx.data.rsam;

import java.util.List;
import java.util.Map;

// import gov.usgs.util.ConfigFile;
// import gov.usgs.vdx.data.BinaryDataSet;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.rsam.RSAMData;
import gov.usgs.vdx.data.rsam.SQLRSAMDataSource;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

/**
 * SQL Data Source for RatSAM Data
 *
 * @author Loren Antolik
 */
public class SQLRatSAMDataSource extends SQLDataSource implements DataSource {
	
	public static final String DATABASE_NAME	= "rsam";
	public static final boolean channels		= true;
	public static final boolean translations	= false;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= false;
	public static final boolean columns			= true;
	SQLRSAMDataSource sqlRSAMDataSource;
	
	/**
	 * Get database type, ratsam in this case
	 */
	public String getType() {
		return "ratsam";
	}
	public boolean getChannelsFlag() { return channels; }
	public boolean getTranslationsFlag() { return translations; }
	public boolean getChannelTypesFlag() { return channelTypes; }
	public boolean getRanksFlag() { return ranks; }
	public boolean getColumnsFlag() { return columns; }
	
	/**
	 * Initialize data source
	 */
	public void initialize(VDXDatabase db, String dbName) {
		defaultInitialize(db, dbName + "$" + getType());
		if (!databaseExists()) {
			createDatabase();
		}
	}

	/**
	 * Get flag if database exist
	 */
	public boolean databaseExists() {
		return defaultDatabaseExists();
	}
	
	/**
	 * Create ratsam database
	 */
	public boolean createDatabase() {
		return true;
	}
	
	/**
	 * TODO: work out the fact that cid is actually a comma separated string of two cid's
	 * Getter for data. 
	 * Search value of 'action' parameter and retrieve corresponding data.
	 * @param command to execute. 
	 */
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		
		if (action == null) {
			return null;
		
		} else if (action.equals("channels")) {
			return new TextResult(defaultGetChannels(channelTypes));
			
		} else if (action.equals("data")) {
			int cid			= Integer.parseInt(params.get("ch"));
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			double period	= Double.parseDouble(params.get("period"));
			RSAMData data	= getRatSAMData(0, 0, st, et, period);
			if (data != null) {
				return new BinaryResult(data);
			}
			
		}
		return null;
	}

	/**
	 * 
	 * @param channel
	 * @param st
	 * @param et
	 * @param period
	 * @return
	 */
	protected RSAMData getRatSAMData(int cid1, int cid2, double st, double et, double period) {
		RSAMData d1 = null;
		RSAMData d2 = null;

		d1 = sqlRSAMDataSource.getRSAMData(cid1, st, et, period);
		d2 = sqlRSAMDataSource.getRSAMData(cid2, st, et, period);
		
		return d1.getRatSAM(d2);
	}
}
