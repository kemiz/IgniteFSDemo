package marketdata.model;


import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Currency {

    public Long getId() {
        return id;
    }

    @QuerySqlField(index = true)
    private final Long id;
    @QuerySqlField(index = true)
    private final String currencyCode;

    public static Currency createNew(Long i, String countryCode) {
        return new Currency(
                i,
                countryCode);
    }

    private Currency(Long id, String currencyCode) {
        this.id = id;
        this.currencyCode = currencyCode;
    }

    public String getCurrency() {
        return currencyCode;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("W6Price{");
        sb.append(", id='").append(id).append('\'');
        sb.append(", currency='").append(currencyCode).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
