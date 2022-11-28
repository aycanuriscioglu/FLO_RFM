import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
import datetime as dt

pd.set_option('display.float_format', lambda x: '%.3f' % x)

df_ = pd.read_csv("flo_data_20k.csv")
df = df_.copy()

df.head()
df.tail()
df.columns
df.describe().T
df.isnull().sum()
df.dtypes


df["master_id"].nunique()

df["of_on_total_ever"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]

df["of_on_total_price_ever"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]
df.head()

df.dtypes
[col for col in df.columns if "date" in col]

date_change = df.columns[df.columns.str.contains("date")]
df[date_change] = df[date_change].apply(pd.to_datetime)
df.dtypes


df.groupby("order_channel").agg({"of_on_total_ever": "sum",
                                 "of_on_total_price_ever": "sum"})


totalprice_top10=df.sort_values(by="of_on_total_price_ever", ascending=False).head(10)
totalprice_top10


totalorder_top10=df.sort_values(by="of_on_total_ever", ascending=False).head(10)
totalorder_top10

def func(x):
    df["of_on_total_ever"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["of_on_total_price_ever"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]

    df['first_order_date'] = pd.to_datetime(df['first_order_date'])
    df['last_order_date'] = pd.to_datetime(df['last_order_date'])
    df['last_order_date_online'] = pd.to_datetime(df['last_order_date_online'])
    df['last_order_date_offline'] = pd.to_datetime(df['last_order_date_offline'])

    return(df)

func(df)

df["last_order_date"].max()

today_date = dt.datetime(2021, 6, 1)
type(today_date)

df.groupby("master_id").agg({"last_order_date": lambda last_order_date: (today_date - last_order_date.max()).days,
                             "of_on_total_ever": lambda of_on_total_ever: of_on_total_ever,
                             "of_on_total_price_ever": lambda of_on_total_price_ever: of_on_total_price_ever})


rfm = df.groupby("master_id").agg({"last_order_date": lambda last_order_date: (today_date - last_order_date.max()).days,
                             "of_on_total_ever": lambda of_on_total_ever: of_on_total_ever,
                             "of_on_total_price_ever": lambda of_on_total_price_ever: of_on_total_price_ever})


rfm.columns = ['recency', 'frequency', 'monetary']

rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])

rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]) 

rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean"])


#WOMAN_BEST_CUSTOMERS

df["interested_in_categories_12"].value_counts()

rfm_c_lc = rfm[(rfm["segment"]=="champions") | (rfm["segment"]=="loyal_customers")]
rfm_c_lc
rfm_c_lc.shape[0]

woman = df[(df["interested_in_categories_12"]).str.contains("KADIN")]

woman_best_customers = pd.merge(rfm_c_lc, woman[["interested_in_categories_12","master_id"]], on=["master_id"])

woman_best_customers= woman_best_customers.drop(woman_best_customers.loc[:,'recency':'interested_in_categories_12'].columns,axis=1)
woman_best_customers

woman_best_customers.to_csv("woman_best_customers.csv")


#TURN_BACK_TO_COMPANY_MAN_CHILD

requested_customers = rfm[(rfm["segment"]=="cant_loose") | (rfm["segment"]=="about_to_sleep") | (rfm["segment"]=="new_customers")]

man_children = df[(df["interested_in_categories_12"]).str.contains("ERKEK|COCUK|AKTIFCOCUK")]

turn_back_to_company = pd.merge(requested_customers, man_children[["interested_in_categories_12","master_id"]],on=["master_id"])

turn_back_to_company = turn_back_to_company.drop(turn_back_to_company.loc[:,'recency':'interested_in_categories_12'].columns,axis=1)
turn_back_to_company.head()
turn_back_to_company.shape
turn_back_to_company.to_csv("turn_back_to_company.csv")
