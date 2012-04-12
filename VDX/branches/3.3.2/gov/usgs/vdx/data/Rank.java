package gov.usgs.vdx.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Loren Antolik
 */
public class Rank {
	private int rid;
	private String name;
	private int rank;
	private int user_default;
	
	public Rank() { }
	
	/**
	 * Constructor
	 * @param rid			rank id
	 * @param name			rank name
	 * @param rank			rank
	 * @param user_default	user default in menu
	 */
	public Rank(int rid, String name, int rank, int user_default) {
		this.rid			= rid;
		this.name			= name;
		this.rank			= rank;
		this.user_default	= user_default;
	}
	
	public Rank(String rk) {
		String[] parts	= rk.split(":");
		rid				= Integer.parseInt(parts[0]);
		name			= parts[1];
		rank			= Integer.parseInt(parts[2]);
		user_default	= Integer.parseInt(parts[3]);
	}
	
	public Rank bestPossible() {
		return new Rank(0, "Best Possible Rank", 0, 0);
	}

	/**
	 * Getter for rank id
	 */
	public int getId() {
		return rid;
	}

	/**
	 * Getter for rank name
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Getter for rank
	 */
	public int getRank() {
		return rank;
	}
	
	/**
	 * Getter for user default
	 */
	public int getUserDefault() {
		return user_default;
	}
	
	/**
	 * Conversion utility
	 * @param ss
	 * @return
	 */
	public static Map<Integer, Rank> fromStringsToMap(List<String> ss) {
		Map<Integer, Rank> map = new HashMap<Integer, Rank>();
		for (String s : ss) {
			Rank rk = new Rank(s);
			map.put(rk.getId(), rk);
		}
		return map;
	}
	
	/**
	 * Conversion of objects to string
	 */
	public String toString() {
		return String.format("%d:%s:%d:%d", getId(), getName(), getRank(), getUserDefault());
	}
}