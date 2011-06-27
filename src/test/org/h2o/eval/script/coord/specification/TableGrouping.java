package org.h2o.eval.script.coord.specification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TableGrouping {

    private final Map<Integer, ArrayList<String>> groupings = new HashMap<Integer, ArrayList<String>>();
    private int totalNumberOfMachines = 0;

    public void addTable(final int locationId, final String tableName) {

        totalNumberOfMachines++;

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

        return totalNumberOfMachines;
    }
}
