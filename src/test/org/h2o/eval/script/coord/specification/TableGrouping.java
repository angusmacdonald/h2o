package org.h2o.eval.script.coord.specification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableGrouping {

    private final Map<Integer, ArrayList<String>> groupings = new HashMap<Integer, ArrayList<String>>();
    private final Set<Integer> totalNumberOfMachines = new HashSet<Integer>();

    public void addTable(final int locationId, final String tableName) {

        totalNumberOfMachines.add(locationId);

        if (groupings.containsKey(locationId)) {
            final ArrayList<String> existingNames = groupings.get(locationId);
            existingNames.add(tableName);
            groupings.put(locationId, existingNames);
        }
        else {
            final ArrayList<String> newList = new ArrayList<String>();
            newList.add(tableName);
            groupings.put(locationId, newList);
        }

    }

    public Map<Integer, ArrayList<String>> getGroupings() {

        return groupings;
    }

    public int getTotalNumberOfMachines() {

        return totalNumberOfMachines.size();
    }
}
