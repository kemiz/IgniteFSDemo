package marketdata.model;


import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Sector {

    @QuerySqlField(index = true)
    private final Long id;
    @QuerySqlField(index = true)
    private final String sectorName;

    public static Sector createNew(Long i, String sectorName) {
        return new Sector(
                i,
                sectorName);
    }

    private Sector(Long id, String sectorName) {
        this.id = id;
        this.sectorName = sectorName;
    }

    public String getSectorName() {
        return sectorName;
    }

    public Long getId() {
        return id;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("W6Price{");
        sb.append(", id='").append(id).append('\'');
        sb.append(", sectorName='").append(sectorName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
