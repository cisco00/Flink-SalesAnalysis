package SalesAnalysis.entities;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderItems {
    public Integer orderId;
    public Integer itemId;
    public Integer productId;
    public Integer quantity;
    public float pricePerUnit;;


}
