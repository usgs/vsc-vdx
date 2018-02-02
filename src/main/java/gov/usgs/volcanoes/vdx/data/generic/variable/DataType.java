package gov.usgs.volcanoes.vdx.data.generic.variable;

/**
 * Represents data type.
 *
 * @author Tom Parker
 */
public class DataType {

  private int stid;
  private String name;

  /**
   * Constructor.
   *
   * @param id data type id
   * @param n name
   */
  public DataType(int id, String n) {
    stid = id;
    name = n;
  }

  /**
   * Setter for data type id.
   *
   * @param i new id
   */
  public void setId(int i) {
    stid = i;
  }

  /**
   * Getter for data type id.
   *
   * @return data type id
   */
  public int getId() {
    return stid;
  }

  /**
   * Getter for data type name.
   *
   * @return data type name
   */
  public String getName() {
    return name;
  }

  /**
   * Getter for data type name.
   *
   * @return data type name
   */
  public String toString() {
    return name;
  }

  /**
   * Compare data types by id.
   *
   * @return true if equal, false otherwise
   */
  public boolean equals(int i) {
    return i == stid ? true : false;
  }
}
