# Databricks notebook source
from pyspark.sql.functions import count, avg, sum, month


# COMMAND ----------

# Load rental table
rental_df = spark.read.csv("/Workspace/Users/partho.21partho@gmail.com/DVD_data/rental.csv", header=True, inferSchema=True)
rental_df.show(5)

# Load payment table
payment_df = spark.read.csv("/Workspace/Users/partho.21partho@gmail.com/DVD_data/payment.csv", header=True, inferSchema=True)
payment_df.show(5)



# COMMAND ----------


# 1. Get total number of payments

total_payments = payment_df.count()
print("Total number of payment : ",total_payments)

# COMMAND ----------

# 2. Get average payment amount
avg_payment = payment_df.select(avg("amount").alias("average_amount"))
avg_payment.show()

# COMMAND ----------

# 3. Aggregate payments by staff and find the best staff (highest total)

payments_by_staff = (
    payment_df
    .groupBy("staff_id")
    .agg(sum("amount").alias("total_collected"))
    .orderBy("total_collected")
)

payments_by_staff.show()


# Find the best staff (highest total amount collected)
best_staff_id = (
    payment_df
    .groupBy("staff_id")
    .agg(sum("amount").alias("total_amount"))
    .orderBy("total_amount", ascending=False)
    .limit(1)
)
print("Best Staff ID : ",best_staff_id.collect()[0][0])
best_staff_id.show()

# COMMAND ----------

# 4. Count rentals per month

rentals_per_month = (
    rental_df
    .withColumn("rental_month", month("rental_date"))
    .groupBy("rental_month")
    .count()
    .orderBy("rental_month")
)

rentals_per_month.show()

# COMMAND ----------

# 5. Total payments collected by each staff(join rental and payment tables)


# Join payment and rental tables
payment_rental_join = (
    payment_df
    .join(rental_df, payment_df.rental_id == rental_df.rental_id)
)

# Aggregate total payments collected by each staff member
total_payment_by_staff = (
    payment_rental_join
    .groupBy(rental_df.staff_id)
    .agg(sum(payment_df.amount).alias("total_collected"))
)

total_payment_by_staff.show()

