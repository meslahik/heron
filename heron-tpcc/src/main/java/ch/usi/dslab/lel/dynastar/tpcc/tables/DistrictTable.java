package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastar.tpcc.rows.District;

import java.util.HashMap;

public class DistrictTable {
    public HashMap<Integer, District> map = new HashMap<>();

    public District get(int districtId) {
        return map.get(districtId);
    }

    public void put(District district) {
        map.put(district.d_id, district);
    }
}
