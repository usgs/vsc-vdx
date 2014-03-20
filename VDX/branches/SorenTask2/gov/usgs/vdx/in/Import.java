package gov.usgs.vdx.in;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.CurrentTime;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.vdx.data.SQLDataSourceHandler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Superclass for importers.  Handles variable declarations
 * @author Loren Antolik (USGS)
 *
 */
public class Import {
	
	public static Set<String> flags;
	public static Set<String> keys;
	
	public String vdxConfig;
	
	public ConfigFile params;
	public ConfigFile vdxParams;
	public ConfigFile rankParams;
	public ConfigFile channelParams;
	public ConfigFile columnParams;
	public ConfigFile dataSourceParams;
	public ConfigFile translationParams;
	
	public String driver, prefix, url;
	
	public SimpleDateFormat dateIn, dateOut;
	public Double j2ksec;
	public Date date;
	
	public String filemask;
	public int headerlines;
	public String delimiter;
	
	public String fields;
	public String[] fieldArray;
	public Map<Integer, String> fieldMap;
	public Map<Integer, String> defaultFieldMap;
	
	public String dataSource;
	public SQLDataSource sqlDataSource;
	public SQLDataSourceHandler sqlDataSourceHandler;
	public SQLDataSourceDescriptor sqlDataSourceDescriptor;	
	public List<String> dataSourceList;
	public Iterator<String> dsIterator;
	public Map<String, SQLDataSource> sqlDataSourceMap;
	public Map<String, String> dataSourceColumnMap;
	public Map<String, String> dataSourceChannelMap;
	public Map<String, Integer>	dataSourceRIDMap;
	
	public Rank rank;
	public String rankName;
	public int rankValue, rankDefault;
	public int rid;

	public Channel channel;	
	public String channelCode, channelName;
	public double channelLon, channelLat, channelHeight;
	public Map<String, Channel> channelMap;
	public List<String> channelList;
	public String channels;
	public String defaultChannels;
	public String[] channelArray;
	public String[] dsChannelArray;
	public String channelFields;
	public Map<String, String> channelFieldMap;
	
	public Column column;
	public String columnName, columnDescription, columnUnit;
	public int columnIdx;
	public boolean columnActive, columnChecked, columnBypass, columnAccumulate;
	public List<Column> columnList;
	public String columns;
	public String[] columnArray;	
	public String defaultColumns;
	
	public List<String> stringList;

	public Logger logger;

	public double azimuthNom;
	public double azimuthInst; 
	
	public CurrentTime currentTime = CurrentTime.getInstance();
	
	static {
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("-v");
	}

}
