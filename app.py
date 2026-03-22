import pandas as pd
import streamlit as st

st.title("📊 Retail Sales Dashboard")

# Load dataset
df = pd.read_csv("data/sales.csv")

# Feature Engineering
df["total_price"] = df["price"] * df["quantity"]

# KPI: Total Revenue
total_revenue = df["total_price"].sum()
st.metric("Total Revenue", f"₹{total_revenue}")

# Category-wise Sales
st.subheader("Category-wise Sales")
category_sales = df.groupby("category")["total_price"].sum().reset_index()
st.dataframe(category_sales)

# City-wise Sales
st.subheader("City-wise Sales")
city_sales = df.groupby("city")["total_price"].sum().reset_index()
st.dataframe(city_sales)