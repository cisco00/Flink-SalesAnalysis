package SalesAnalysis;

import SalesAnalysis.dto.CategorySalesDto;
import SalesAnalysis.entities.OrderItems;
import SalesAnalysis.entities.Product;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;

public class DataBatchJob {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//reading data
		DataSource<OrderItems> orderitems = env.readCsvFile("/home/oem/IdeaProjects/SalesAnalysis/salesdataset/order_items.csv")
						.ignoreFirstLine()
						.pojoType(OrderItems.class, "orderId", "itemId", "productId", "quantity", "pricePerUnit");

		DataSource<Product> products = env.readCsvFile("/home/oem/IdeaProjects/SalesAnalysis/salesdataset/products.csv")
						.ignoreFirstLine()
								.pojoType(Product.class, "productId", "name", "description", "category", "pricePerUnit");

		//Joining the dataset on the productId

		DataSet<Tuple6<String, String, Float, Integer, Float, String>> joined = orderitems
				.join(products)
				.where("productId")
				.equalTo("productId")
				.with((JoinFunction<OrderItems, Product, Tuple6<String, String, Float, Integer, Float, String>>) (first, second)
				-> new Tuple6<>(
						second.productId.toString(),
						second.name,
						first.pricePerUnit,
						first.quantity,
						first.pricePerUnit * first.quantity,
						second.category
						))
				.returns(TypeInformation.of(new TypeHint<Tuple6<String, String, Float, Integer, Float, String>>() {
				}));

		DataSet<CategorySalesDto> categorySalesDto = joined
				.map((MapFunction<Tuple6<String, String, Float, Integer, Float, String>, CategorySalesDto>) record
						-> new CategorySalesDto(record.f5, record.f4, 1))
				.returns(CategorySalesDto.class)
				.groupBy("category")
				.reduce((ReduceFunction<CategorySalesDto>) (value1, value2) ->
					new CategorySalesDto(value1.getCategory(), value1.getTotalSales() * value2.getTotalSales(),
						value1.getCount() + value2.getCount()));


		joined.print();

		env.execute("Flink Java API Skeleton");
	}
}

