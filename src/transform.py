from pyspark.sql.functions import col, sum as _sum, rank, lag
from pyspark.sql.window import Window

def run_pipeline(sales_df, customers_df, products_df):

    sales_df = sales_df.dropna()

    df = sales_df.join(customers_df, on="customer_id", how="left")
    df = df.join(products_df, on="product_id", how="left")

    df = df.withColumn("total_price", col("quantity") * col("price"))

    category_sales = df.groupBy("category").agg(
        _sum("total_price").alias("total_sales")
    )

    window_spec = Window.orderBy(col("total_sales").desc())
    ranked_df = category_sales.withColumn("rank", rank().over(window_spec))

    lag_window = Window.orderBy("total_sales")
    ranked_df = ranked_df.withColumn(
        "previous_sales",
        lag("total_sales", 1).over(lag_window)
    )

    return ranked_df
