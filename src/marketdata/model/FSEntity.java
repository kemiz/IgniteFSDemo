package marketdata.model;


import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class FSEntity {

    @QuerySqlField
    private final String batch;
    @QuerySqlField(index = true)
    private final Long id;
    @QuerySqlField
    private final String issueCountry;
    @QuerySqlField (index = true)
    private final String sector;
    @QuerySqlField
    private final String billingCode;
    @QuerySqlField (index = true)
    private final String currencyCode;
    @QuerySqlField
    private final String prepaymentType;
    @QuerySqlField
    private final Long liquidityScore;

    private transient AffinityKey<Long> key;

    public static FSEntity createNew(Long i, String issueCountry, String currencyCode, String sector) {
        return new FSEntity(
                "batch" + i,
                i,
                issueCountry,
                sector,
                "billingCode" +i,
                currencyCode,
                "prepayment" + i,
                i);
    }

    private FSEntity(String batch, Long id, String issueCountry, String sector, String billingCode, String currencyCode, String prepaymentType, Long liquidityScore) {
        this.batch = batch;
        this.id = id;
        this.issueCountry = issueCountry;
        this.sector = sector;
        this.billingCode = billingCode;
        this.currencyCode = currencyCode;
        this.prepaymentType = prepaymentType;
        this.liquidityScore = liquidityScore;
    }

    public String getBatch() {
        return batch;
    }

    public AffinityKey<Long> key() {
        if (key == null)
            key = new AffinityKey<Long>(id, id);
        return key;
    }

    public String getIssueCountry() {
        return issueCountry;
    }

    public String getSector() {
        return sector;
    }

    public String getBillingCode() {
        return billingCode;
    }

    public String getCurrency() {
        return currencyCode;
    }

    public String getPrepaymentType() {
        return prepaymentType;
    }

    public Long getLiquidityScore() {
        return liquidityScore;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("W6Price{");
        sb.append("batch='").append(batch).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", issueCountry='").append(issueCountry).append('\'');
        sb.append(", sector='").append(sector).append('\'');
        sb.append(", billingCode='").append(billingCode).append('\'');
        sb.append(", currency='").append(currencyCode).append('\'');
        sb.append(", prepaymentType='").append(prepaymentType).append('\'');
        sb.append(", liquidityScore=").append(liquidityScore);
        sb.append('}');
        return sb.toString();
    }
}
