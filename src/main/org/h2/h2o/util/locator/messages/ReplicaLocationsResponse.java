package org.h2.h2o.util.locator.messages;

import java.util.List;
import java.util.Set;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ReplicaLocationsResponse {
	private List<String> locations;
	private int updateCount;
	
	
	/**
	 * @param locations
	 * @param updateCount
	 */
	public ReplicaLocationsResponse(List<String> locations, int updateCount) {
		super();
		this.locations = locations;
		this.updateCount = updateCount;
	}
	
	/**
	 * @return the locations
	 */
	public List<String> getLocations() {
		return locations;
	}
	/**
	 * @return the updateCount
	 */
	public int getUpdateCount() {
		return updateCount;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 31 + ((locations == null) ? 0 : locations.hashCode());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ReplicaLocationsResponse other = (ReplicaLocationsResponse) obj;
		if (locations == null) {
			if (other.locations != null)
				return false;
		} else if (!locations.equals(other.locations))
			return false;
		return true;
	}
	
	
}
