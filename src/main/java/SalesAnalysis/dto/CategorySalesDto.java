package SalesAnalysis.dto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CategorySalesDto {
    public String category;
    public float totalSales;
    public Integer count;
}
