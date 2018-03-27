package gov.usgs.volcanoes.vdx.in.hw;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.time.CurrentTime;
import gov.usgs.volcanoes.core.time.Time;
import gov.usgs.volcanoes.core.util.StringUtils;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for the Sick DT1000 Long Range Distance Sensor.
 *
 * @author Bill Tollett
 */
public class SickDT1000 implements Device {

  private static final int MIN_MESSAGE_LENGTH = 16;
  private static final Logger LOGGER = LoggerFactory.getLogger(SickDT1000.class);

  /**
   * the timestamp mask.
   */
  protected String timestamp;

  /**
   * the timezone.
   */
  protected String timezone;

  /**
   * the connection timout.
   */
  protected int timeout;

  /**
   * the maximum number of tries.
   */
  protected int maxtries;

  /**
   * the sample rate of the device, seconds per acquisition.
   */
  protected int samplerate;

  /**
   * the delimeter of the data.
   */
  protected String delimiter;

  /**
   * the column to check for null in database.
   */
  protected String nullfield;

  /**
   * flag to set last data time to system default or NOW.
   */
  protected boolean pollhist;

  /**
   * the columns available on the device.
   */
  protected String fields;

  /**
   * the id of the station.
   */
  protected String id;

  /**
   * Initialize the hardware device driver.
   * @param params ConfigFile
   * @throws Exception if the fields aren't defined
   */
  @Override
  public void initialize(ConfigFile params) throws Exception {
    id = StringUtils.stringToString(params.getString("id"), "0");
    timestamp = StringUtils.stringToString(params.getString("timestamp"), "MM/dd/yy HH:mm:ss");
    timezone = StringUtils.stringToString(params.getString("timezone"), "GMT");
    timeout = StringUtils.stringToInt(params.getString("timeout"), 60000);
    maxtries = StringUtils.stringToInt(params.getString("maxtries"), 2);
    samplerate = StringUtils.stringToInt(params.getString("samplerate"), 60);
    delimiter = StringUtils.stringToString(params.getString("delimiter"), ",");
    nullfield = StringUtils.stringToString(params.getString("nullfield"), "");
    pollhist = StringUtils.stringToBoolean(params.getString("pollhist"), false);
    fields = StringUtils.stringToString(params.getString("fields"), "");

    // validation
    if (fields.length() == 0) {
      throw new Exception("fields not defined");
    }
  }

  /**
   * This is a streaming device, so data is never explicity requested from it.
   * @param startDate Date to start from
   * @return Request string
   * @throws Exception Never.
   */
  @Override
  public String requestData(Date startDate) throws Exception {
    return "";
  }

  /**
   * Determine whether or not a message is complete. A completed message is defined as finishing
   * with \r\n
   * @param message Raw message from the device.
   * @return Whether or not this is a complete message.
   * @throws Exception Never
   */
  @Override
  public boolean messageCompleted(String message) throws Exception {
    if (message.substring(message.length() - 2).equals("\r\n")) {
      return true;
    }
    return false;
  }

  /**
   * Validate that the message received from the device is complete.
   *
   * @param message Raw message from the device.
   * @param ignoreWrongAddress Not used.
   * @return Whether or not the message is valid.
   * @throws Exception If the message is of the incorrect length, doesn't start with a sign, or
   *                   end with \r\n
   */
  @Override
  public boolean validateMessage(String message, boolean ignoreWrongAddress) throws Exception {
    LOGGER.debug("validatingMessage: {}", message);

    // Check for message length
    int len = message.length();
    if (len < MIN_MESSAGE_LENGTH) {
      throw new Exception("Too short. Len = " + len + "\n" + message);
    }

    // Check that the message starts with a sign
    if (!(message.startsWith("+") || message.startsWith("-"))) {
      throw new Exception("Incomplete message. Missing starting sign.\n" + message);
    }

    // Check that the message ends with \r\n
    if (!message.endsWith("\r\n")) {
      throw new Exception("Incomplete message. Missing trailing \\r\\n\n" + message);
    }

    return true;
  }

  /**
   * Validation happens in the validateMesage method, so there's nothing to do here.
   *
   * @param line Formatted line from the device.
   * @throws Exception Never thrown.
   */
  @Override
  public void validateLine(String line) throws Exception {
    // Handled in validateMessage I hope
  }

  /**
   * Format the raw message coming in from the device.
   *
   * @param message A single line of input from the device.
   * @return Message with surrouding formatting removed.
   */
  @Override
  public String formatMessage(String message) {
    // Message is in format <sign><7*[0-9]>_<5*[0-9]><CR><LF>
    // If we've gotten this far, we know that the message starts with a sign and ends with \r\n.

    // Remove the sign at the beginning
    message = message.substring(1);

    // Remove the newline
    message = message.substring(0, message.length() - 2);

    LOGGER.debug("Message Formatted: {}", message);

    return message;
  }

  /**
   * Given a single line of data from the device, calculate values to be inserted into database.
   *
   * @param line Single line of data with its formatting removed.
   * @return String containing date, sea level value, overlook value, signal strength
   */
  @Override
  public String formatLine(String line) {
    // Return date, mAbvSL, mRelOVL, sigStr
    String[] split = line.split("_");
    StringBuilder result = new StringBuilder();

    // Grab time
    result.append(Time.toDateString(CurrentTime.getInstance().now()));

    // Compute the meters above sea level
    double raw = Double.valueOf(split[0]);
    double meters = Double.NaN;
    double ovl = Double.NaN;
    if (raw != 6096000.0) {
      meters = raw / 1000.0;
      meters = meters * Math.sin(Math.toRadians(54.7));
      meters = 1107 - meters;
      ovl = (1131 - meters) * -1;
    }
    result.append(",").append(meters).append(",").append(ovl);

    // Add the signal strength
    result.append(",").append(Integer.valueOf(split[1]));

    return result.toString();
  }

  /**
   * Setter for time value.
   */
  @Override
  public String setTime() {
    return null;
  }

  /**
   * Getter for timestamp format value.
   */
  @Override
  public String getTimestamp() {
    return timestamp;
  }

  /**
   * Getter for timezone value.
   */
  @Override
  public String getTimezone() {
    return timezone;
  }

  /**
   * Getter for delimeter value.
   */
  @Override
  public String getDelimiter() {
    return delimiter;
  }

  /**
   * Getter for fields value.
   */
  @Override
  public String getFields() {
    return fields;
  }

  /**
   * Getter for nullfield value.
   */
  @Override
  public String getNullfield() {
    return nullfield;
  }

  /**
   * Getter for pollhist value.
   */
  @Override
  public boolean getPollhist() {
    return pollhist;
  }

  /**
   * Getter for timeout value.
   */
  @Override
  public int getTimeout() {
    return timeout;
  }

  /**
   * Get the maximum number of tries.
   */
  @Override
  public int getMaxtries() {
    return maxtries;
  }
}
