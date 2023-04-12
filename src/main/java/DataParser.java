import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Objects;

public class DataParser {

  private static CSVReader csvReader;
  private static StringReader strReader;
  public static final String delimiter = "\t";

  // For neighborhood.csv
  public Record getSingleRecord(String line) throws IOException, CsvValidationException{
    strReader = new StringReader(line);
    csvReader = new CSVReader(strReader);

    String[] values = csvReader.readNext();

    String year = "invalid";
    if (values[1] != null) {
      year = values[1].substring(0,4);
    }

    String state = "invalid";
    if (values[10] != null) {
      state = values[10].trim();
    }

    String propertyTypeId = "invalid";
    if (values[12] != null) {
      propertyTypeId = values[12].trim();
    }

    double salePrice = -100;
    if (!Objects.equals(values[13], "")) {
      salePrice = Double.parseDouble(values[13]);
    }

    return new Record(year, state, propertyTypeId, salePrice);
  }

  // For zipcode.csv
  public Record getSingleRecord1(String line) throws IOException, CsvValidationException{
    strReader = new StringReader(line);
    csvReader = new CSVReader(strReader);

    String[] values = csvReader.readNext();

    String year = "invalid";
    if (values[0].split(delimiter)[1] != null) {
      year = values[0].split(delimiter)[1].substring(0,4);
    }

    String state = "invalid";
    if (values[0].split(delimiter)[10] != null) {
      state = values[0].split(delimiter)[10].trim();
    }

    String propertyTypeId = "invalid";
    if (values[0].split(delimiter)[12] != null) {
      propertyTypeId = values[0].split(delimiter)[12].trim();
    }

    double salePrice = -100;
    if (!Objects.equals(values[0].split(delimiter)[13], "")) {
      salePrice = Double.parseDouble(values[0].split(delimiter)[13]);
    }

    return new Record(year, state, propertyTypeId, salePrice);
  }
}
