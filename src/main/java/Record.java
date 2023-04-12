public class Record {

  private String year, state, propertyTypeId;
  private double salePrice;

  public Record(String year, String state, String propertyTypeId, double salePrice) {
    this.year = year;
    this.state = state;
    this.propertyTypeId = propertyTypeId;
    this.salePrice = salePrice;
  }

  public String getYear() {
    return year;
  }

  public void setYear(String year) {
    this.year = year;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getPropertyTypeId() {
    return propertyTypeId;
  }

  public void setPropertyTypeId(String propertyTypeId) {
    this.propertyTypeId = propertyTypeId;
  }

  public double getSalePrice() {
    return salePrice;
  }

  public void setSalePrice(double salePrice) {
    this.salePrice = salePrice;
  }

  @Override
  public String toString() {
    return "Record{" +
        "year='" + year + '\'' +
        ", state='" + state + '\'' +
        ", propertyTypeId='" + propertyTypeId + '\'' +
        ", salePrice=" + salePrice +
        '}';
  }
}
