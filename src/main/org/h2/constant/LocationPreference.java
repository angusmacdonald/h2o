package org.h2.constant;

/**
 * Specifies where the user would like a query to be directed (i.e. to which replica.)
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocationPreference {
	
	public static LocationPreference LOCAL, PRIMARY, NO_PREFERENCE, LOCAL_STRICT, PRIMARY_STRICT, NO_PREFERENCE_STRICT;
	
	static {
		LOCAL = new LocationPreference("LOCAL", false);
		PRIMARY = new LocationPreference("PRIMARY", false);
		NO_PREFERENCE = new LocationPreference("NO_PREFERENCE", false);
		
		LOCAL_STRICT = new LocationPreference("LOCAL", true);
		PRIMARY_STRICT = new LocationPreference("PRIMARY", true);
		NO_PREFERENCE_STRICT = new LocationPreference("NO_PREFERENCE", true);
	}
	
	private String setting;
	
	private boolean strictSetting;
	
	private LocationPreference(String setting, boolean isStrict) {
		this.setting = setting;
		this.strictSetting = isStrict;
	}
	
	public boolean isStrict() {
		return strictSetting;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ( ( setting == null ) ? 0 : setting.hashCode() );
		result = prime * result + ( strictSetting ? 1231 : 1237 );
		return result;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if ( this == obj )
			return true;
		if ( obj == null )
			return false;
		if ( getClass() != obj.getClass() )
			return false;
		LocationPreference other = (LocationPreference) obj;
		if ( setting == null ) {
			if ( other.setting != null )
				return false;
		} else if ( !setting.equals(other.setting) ) {
			return false;
		}
		// if (strictSetting != other.strictSetting)
		// return false;
		return true;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return setting + ( ( strictSetting ) ? ", STRICT" : "" );
	}
	
}
