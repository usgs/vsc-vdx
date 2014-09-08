package gov.usgs.vdx.calc.data;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Data structure used in Hypo71 algorithm.
 * 
 * @author Oleg Shepelev
 */
@XmlRootElement
public class Station  implements Serializable{
	/**
	 * If IW = *, then this station has zero weight assigned to its P and/or S reading(s).
	 */
	char IW;
	/**
	 * Station name.
	 */
	String NSTA;
	/**
	 * Degree portion of latitude.
	 */
	int LAT1;
	/**
	 * Minute portion of latitude.
	 */
	double LAT2;
	/**
	 * Punch N or leave this column blank for stations in northern hemisphere. Punch S for stations in sourthern hemisphere.
	 */
	char INS;
	/**
	 * Degree portion of longitude.
	 */
	int LON1;
	/**
	 * Minute portion of longitude.
	 */
	double LON2;
	/**
	 * Punch E for eastern longitude. W or blank for western.
	 */
	char IEW;
	/**
	 * Elevation in meters. This data is not used in the program.
	 */
	int IELV;
	double dly;
	/**
	 * Station delay in seconds.
	 */
	double FMGC;
	/**
	 * Station correction for FMAG.
	 */
	double XMGC;
	/**
	 * System number is assigned for each station so that the frequency response curve of the seismometer and preamp is specified for the amplitude magnitude calculation (XMAG).
	 */
	int KLAS;
	/**
	 * Standard period for XMAG.
	 */
	double PRR;
	/**
	 * Standard calibration for XMAG.
	 */
	double CALR;
	/**
	 * Calibration indicator.
	 */
	int ICAL;
	/**
	 * Year, month and day.
	 */
	int NDATE;
	/**
	 * Hour and minute.
	 */
	int NHRMN;

	// Missing special fields
	// NSTA[I] - NSTA, DELI - dly, AZI, EXGAP, RDGAP
	double AZI;
	double EXGAP;
	double RDGAP;

	// Station output
	// String STN - NSTA
	/**
	 * Epicentral distance in km.
	 */
	double DIST;
	// AZM - AZI
	/**
	 * Angle of incidence measured with respect to downward vertical.
	 */
	int AIN;
	/**
	 * PRMK from input data.
	 */
	String PRMK;
	/**
	 * Hour of arrival time from input data.
	 */
	int HR;
	/**
	 * Minute of arraival time from inout data.
	 */
	int MN;
	/**
	 * The second's portial of P-arrival time from input data.
	 */
	double PSEC;
	/**
	 * Observed P-travet time in sec.
	 */
	double TPOBS;
	/**
	 * Calculated travel time in sec/
	 */
	double TPCAL;
	/**
	 * If the Station Delay Model is used, then DLY means the station delay in
	 * sec from the input list. If the Variable First-Layer Model is used, then
	 * H1 means the thickness of the first layer in km at this station.
	 */
	double DLYH1;
	double PRES;
	double PWT;
	double AMX;
	double PRX;
	double CALX;
	int K;
	double XMAG;
	double RMK;
	double FMP;
	double FMAG;
	String SRMK;
	double SSEC;
	double TSOBS;
	double SRES;
	double SWT;
	String DT;

	// Field for summary
	TravelTimeResiduals model1;
	TravelTimeResiduals model2;
	int NXM;
	double AVXM;
	double SDXM;
	int NFM;
	double AVFM;
	double SDFM;

	@XmlRootElement
	public static class TravelTimeResiduals implements Serializable{
		private int NRES;
		private double SRWT;
		private double AVRES;
		private double SDRES;

		@XmlElement
		public int getNRES() {
			return NRES;
		}

		public void setNRES(int nRES) {
			NRES = nRES;
		}

		@XmlElement
		public double getSRWT() {
			return SRWT;
		}

		public void setSRWT(double sRWT) {
			SRWT = sRWT;
		}

		@XmlElement
		public double getAVRES() {
			return AVRES;
		}

		public void setAVRES(double aVRES) {
			AVRES = aVRES;
		}

		@XmlElement		
		public double getSDRES() {
			return SDRES;
		}

		public void setSDRES(double sDRES) {
			SDRES = sDRES;
		}

		public TravelTimeResiduals(int nRES, double sRWT, double aVRES,
				double sDRES) {
			super();
			NRES = nRES;
			SRWT = sRWT;
			AVRES = aVRES;
			SDRES = sDRES;
		}

		public TravelTimeResiduals(){
			
		}
	}
	public Station(){
		
	}
	public Station(String NSTA, int NRES, double SRWT, double AVRES,
			double SDRES, int NRES1, double SRWT1, double AVRES1,
			double SDRES1, int NXM, double AVXM, double SDXM, int NFM,
			double AVFM, double SDFM) {
		model1 = new TravelTimeResiduals(NRES, SRWT, AVRES, SDRES);
		model2 = new TravelTimeResiduals(NRES1, SRWT1, AVRES1, SDRES1);
		this.NXM = NXM;
		this.AVXM = AVXM;
		this.SDXM = SDXM;
		this.NFM = NFM;
		this.AVFM = AVFM;
		this.SDFM = SDFM;
	}

	public Station(String string, double d, int iAZ, int iAIN, String string2,
			int jHR, int i, double e, double tPK, double f, double dLYK,
			String x4kout, String string3, double g, int iAMX, int iPRX,
			double h, int j, String xMAGOU, char rMK3, String string4,
			String fMPOUT, String fMAGOU, char rMK4, String string5,
			String sKOUT, String tSKOUT, String sRESOU, String rMK5,
			String sWTOUT, String dTKOUT, char c) {
		// TODO Auto-generated constructor stub
	}

	public Station(String nSTA, double dIST, int aIN, String pRMK, int hR,
			int mN, double pSEC, double tPOBS, double tPCAL, double dLYH1,
			String pRES, String pWT, double aMX, double pRX, double cALX,
			int k, double xMAG, double rMK, double fMP, double fMAG,
			String sRMK, double sSEC, double tSOBS, double sRES, double sWT,
			String dT) {
		super();
		NSTA = nSTA;
		DIST = dIST;
		AIN = aIN;
		PRMK = pRMK;
		HR = hR;
		MN = mN;
		PSEC = pSEC;
		TPOBS = tPOBS;
		TPCAL = tPCAL;
		DLYH1 = dLYH1;
		PRES = Double.valueOf(pRES);
		PWT = Double.valueOf(pWT);
		AMX = aMX;
		PRX = pRX;
		CALX = cALX;
		K = k;
		XMAG = xMAG;
		RMK = rMK;
		FMP = fMP;
		FMAG = fMAG;
		SRMK = sRMK;
		SSEC = sSEC;
		TSOBS = tSOBS;
		SRES = sRES;
		SWT = sWT;
		DT = dT;
	}

	public Station(String nSTA, double dly, double aZI, double eXGAP,
			double rDGAP) {
		this.NSTA = nSTA;
		this.dly = dly;
		this.AZI = aZI;
		this.EXGAP = eXGAP;
		this.RDGAP = rDGAP;
	}
	
	public Station(char iW, String nSTA, int lAT1, double lAT2, char iNS,
			int lON1, double lON2, char iEW, int iELV, double dly, double fMGC,
			double xMGC, int kLAS, double pRR, double cALR, int iCAL,
			int nDATE, int nHRMN) {
		IW = iW;
		NSTA = nSTA;
		LAT1 = lAT1;
		LAT2 = lAT2;
		INS = iNS;
		LON1 = lON1;
		LON2 = lON2;
		IEW = iEW;
		IELV = iELV;
		this.dly = dly;
		FMGC = fMGC;
		XMGC = xMGC;
		KLAS = kLAS;
		PRR = pRR;
		CALR = cALR;
		ICAL = iCAL;
		NDATE = nDATE;
		NHRMN = nHRMN;
	}
	
	public Station(char iW, String nSTA, int lAT1, double lAT2, char iNS,
			int lON1, double lON2, char iEW, int iELV, int mNO, double dly, double dly1,
			double xMGC, double fMGC, int kLAS, double pRR, int iCAL,
			int nDATE, int nHRMN) {
		IW = iW;
		NSTA = nSTA;
		LAT1 = lAT1;
		LAT2 = lAT2;
		INS = iNS;
		LON1 = lON1;
		LON2 = lON2;
		IEW = iEW;
		IELV = iELV;
		this.dly = dly;
		FMGC = fMGC;
		XMGC = xMGC;
		KLAS = kLAS;
		PRR = pRR;
		NDATE = nDATE;
		NHRMN = nHRMN;
		ICAL = iCAL;
	}

	@XmlElement
	public char getIW() {
		return IW;
	}

	public void setIW(char iW) {
		IW = iW;
	}

	@XmlElement
	public String getNSTA() {
		return NSTA;
	}

	public void setNSTA(String nSTA) {
		NSTA = nSTA;
	}

	@XmlElement
	public int getLAT1() {
		return LAT1;
	}

	public void setLAT1(int lAT1) {
		LAT1 = lAT1;
	}

	@XmlElement
	public double getLAT2() {
		return LAT2;
	}

	public void setLAT2(double lAT2) {
		LAT2 = lAT2;
	}

	@XmlElement
	public char getINS() {
		return INS;
	}

	public void setINS(char iNS) {
		INS = iNS;
	}

	@XmlElement
	public int getLON1() {
		return LON1;
	}

	public void setLON1(int lON1) {
		LON1 = lON1;
	}

	@XmlElement
	public double getLON2() {
		return LON2;
	}

	public void setLON2(double lON2) {
		LON2 = lON2;
	}

	@XmlElement
	public char getIEW() {
		return IEW;
	}

	public void setIEW(char iEW) {
		IEW = iEW;
	}

	@XmlElement
	public int getIELV() {
		return IELV;
	}

	public void setIELV(int iELV) {
		IELV = iELV;
	}

	@XmlElement
	public double getDly() {
		return dly;
	}

	public void setDly(double dly) {
		this.dly = dly;
	}

	@XmlElement
	public double getFMGC() {
		return FMGC;
	}

	public void setFMGC(double fMGC) {
		FMGC = fMGC;
	}

	@XmlElement
	public double getXMGC() {
		return XMGC;
	}

	public void setXMGC(double xMGC) {
		XMGC = xMGC;
	}

	@XmlElement
	public int getKLAS() {
		return KLAS;
	}

	public void setKLAS(int kLAS) {
		KLAS = kLAS;
	}

	@XmlElement
	public double getPRR() {
		return PRR;
	}

	public void setPRR(double pRR) {
		PRR = pRR;
	}

	@XmlElement
	public double getCALR() {
		return CALR;
	}

	public void setCALR(double cALR) {
		CALR = cALR;
	}

	@XmlElement
	public int getICAL() {
		return ICAL;
	}

	public void setICAL(int iCAL) {
		ICAL = iCAL;
	}

	@XmlElement
	public int getNDATE() {
		return NDATE;
	}

	public void setNDATE(int nDATE) {
		NDATE = nDATE;
	}

	@XmlElement
	public int getNHRMN() {
		return NHRMN;
	}

	public void setNHRMN(int nHRMN) {
		NHRMN = nHRMN;
	}

	@XmlElement
	public double getAZI() {
		return AZI;
	}

	public void setAZI(double aZI) {
		AZI = aZI;
	}

	@XmlElement
	public double getEXGAP() {
		return EXGAP;
	}

	public void setEXGAP(double eXGAP) {
		EXGAP = eXGAP;
	}

	@XmlElement
	public double getRDGAP() {
		return RDGAP;
	}

	public void setRDGAP(double rDGAP) {
		RDGAP = rDGAP;
	}

	@XmlElement
	public double getDIST() {
		return DIST;
	}

	public void setDIST(double dIST) {
		DIST = dIST;
	}

	@XmlElement
	public int getAIN() {
		return AIN;
	}

	public void setAIN(int aIN) {
		AIN = aIN;
	}

	@XmlElement
	public String getPRMK() {
		return PRMK;
	}

	public void setPRMK(String pRMK) {
		PRMK = pRMK;
	}

	@XmlElement
	public int getHR() {
		return HR;
	}

	public void setHR(int hR) {
		HR = hR;
	}

	@XmlElement
	public int getMN() {
		return MN;
	}

	public void setMN(int mN) {
		MN = mN;
	}

	@XmlElement
	public double getPSEC() {
		return PSEC;
	}

	public void setPSEC(double pSEC) {
		PSEC = pSEC;
	}

	@XmlElement
	public double getTPOBS() {
		return TPOBS;
	}

	public void setTPOBS(double tPOBS) {
		TPOBS = tPOBS;
	}

	@XmlElement
	public double getTPCAL() {
		return TPCAL;
	}

	public void setTPCAL(double tPCAL) {
		TPCAL = tPCAL;
	}

	@XmlElement
	public double getDLYH1() {
		return DLYH1;
	}

	public void setDLYH1(double dLYH1) {
		DLYH1 = dLYH1;
	}

	@XmlElement
	public double getPRES() {
		return PRES;
	}

	public void setPRES(double pRES) {
		PRES = pRES;
	}

	@XmlElement
	public double getPWT() {
		return PWT;
	}

	public void setPWT(double pWT) {
		PWT = pWT;
	}

	@XmlElement
	public double getAMX() {
		return AMX;
	}

	public void setAMX(double aMX) {
		AMX = aMX;
	}

	@XmlElement
	public double getPRX() {
		return PRX;
	}

	public void setPRX(double pRX) {
		PRX = pRX;
	}

	@XmlElement
	public double getCALX() {
		return CALX;
	}

	public void setCALX(double cALX) {
		CALX = cALX;
	}

	@XmlElement
	public int getK() {
		return K;
	}

	public void setK(int k) {
		K = k;
	}

	@XmlElement
	public double getXMAG() {
		return XMAG;
	}

	public void setXMAG(double xMAG) {
		XMAG = xMAG;
	}

	@XmlElement
	public double getRMK() {
		return RMK;
	}

	public void setRMK(double rMK) {
		RMK = rMK;
	}

	@XmlElement
	public double getFMP() {
		return FMP;
	}

	public void setFMP(double fMP) {
		FMP = fMP;
	}

	@XmlElement
	public double getFMAG() {
		return FMAG;
	}

	public void setFMAG(double fMAG) {
		FMAG = fMAG;
	}

	@XmlElement
	public String getSRMK() {
		return SRMK;
	}

	public void setSRMK(String sRMK) {
		SRMK = sRMK;
	}

	@XmlElement
	public double getSSEC() {
		return SSEC;
	}

	public void setSSEC(double sSEC) {
		SSEC = sSEC;
	}

	@XmlElement
	public double getTSOBS() {
		return TSOBS;
	}

	public void setTSOBS(double tSOBS) {
		TSOBS = tSOBS;
	}

	@XmlElement
	public double getSRES() {
		return SRES;
	}

	public void setSRES(double sRES) {
		SRES = sRES;
	}

	@XmlElement
	public double getSWT() {
		return SWT;
	}

	public void setSWT(double sWT) {
		SWT = sWT;
	}

	@XmlElement
	public String getDT() {
		return DT;
	}

	public void setDT(String dT) {
		DT = dT;
	}

	@XmlElement
	public TravelTimeResiduals getModel1() {
		return model1;
	}

	public void setModel1(TravelTimeResiduals model1) {
		this.model1 = model1;
	}

	@XmlElement
	public TravelTimeResiduals getModel2() {
		return model2;
	}

	public void setModel2(TravelTimeResiduals model2) {
		this.model2 = model2;
	}

	@XmlElement
	public int getNXM() {
		return NXM;
	}

	public void setNXM(int nXM) {
		NXM = nXM;
	}

	@XmlElement
	public double getAVXM() {
		return AVXM;
	}

	public void setAVXM(double aVXM) {
		AVXM = aVXM;
	}

	@XmlElement
	public double getSDXM() {
		return SDXM;
	}

	public void setSDXM(double sDXM) {
		SDXM = sDXM;
	}

	@XmlElement
	public int getNFM() {
		return NFM;
	}

	public void setNFM(int nFM) {
		NFM = nFM;
	}

	@XmlElement
	public double getAVFM() {
		return AVFM;
	}

	public void setAVFM(double aVFM) {
		AVFM = aVFM;
	}

	@XmlElement
	public double getSDFM() {
		return SDFM;
	}

	public void setSDFM(double sDFM) {
		SDFM = sDFM;
	}
	
	
}