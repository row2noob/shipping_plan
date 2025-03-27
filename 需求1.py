import pandas as pd
import numpy as np
import datetime

pd.set_option('display.float_format', '{:,.2f}'.format)
pd.set_option('future.no_silent_downcasting', True)
# 自动检测当前年月
current_year = datetime.datetime.now().year
current_month = datetime.datetime.now().month
current_month_str = f"{current_year}.{str(current_month).zfill(2)}"

# 四个目标部门
selected_departments = ["亚太", "亚非", "欧洲事业部", "美洲大区"]
months = [f"{current_year}.{str(m).zfill(2)}" for m in range(1, current_month + 1)]

all_data = []
current_month_data = None

for month in months:
    try:
        df = xl("A1:T1000", headers=True, sheet_name=month, book_url="www.365.kdocs.com/your-file")
        df.columns = [
            "部门", "类目", "业务员", "本月目标", "客户名称", "新连锁开发-区域", "订单号", "订单金额(美金)",
            "预计开单金额", "已开单金额", "已开船金额", "汇率", "产品交期", "装柜日期", "开船日期",
            "发货最新进度", "客户型号", "数量", "单价", "备注"
        ]
        df["类目"] = df["类目"].bfill()
        df = df[df["部门"].isin(selected_departments)]

        # 替换空格为0，确保金额字段格式正确
        for col in ["本月目标", "订单金额(美金)", "预计开单金额", "已开单金额", "已开船金额"]:
            df[col] = pd.to_numeric(df[col].replace(r"^\s*$", "", regex=True), errors='coerce').fillna(0)

        df["月份"] = month
        if month == current_month_str:
            current_month_data = df.copy()
        all_data.append(df)
    except Exception as e:
        print(f"⚠️ {month} 数据读取失败: {e}")

df_all = pd.concat(all_data, ignore_index=True)

# ===== 累计数据 =====
df_orders = df_all[df_all["类目"] == "本月预计发货订单"]
cum_summary = df_all.groupby("部门").agg(
    累计任务量=("本月目标", "sum"),
    累计已开船金额=("已开船金额", "sum")
).reset_index()
cum_orders = df_orders.groupby("部门").agg(
    累计已开单金额=("已开单金额", "sum")
).reset_index()
cum_summary = cum_summary.merge(cum_orders, on="部门", how="left").fillna(0)
cum_summary["累计船期完成率(%)"] = cum_summary.apply(
    lambda r: r["累计已开船金额"] / r["累计已开单金额"] * 100
    if r["累计已开单金额"] > 0 else 0, axis=1
)
cum_summary["累计开单完成率(%)"] = cum_summary.apply(
    lambda r: r["累计已开单金额"] / r["累计任务量"] * 100
    if r["累计任务量"] > 0 else 0, axis=1
)

# ===== 当月数据 + 加权船期完成率 =====
df_orders_month = current_month_data[current_month_data["类目"] == "本月预计发货订单"] \
    if current_month_data is not None else pd.DataFrame()

valid_rows = df_orders_month[
    df_orders_month["已开单金额"].notna() & (df_orders_month["已开单金额"] != 0)
].copy()

# 按部门计算当月任务/开单/开船
month_summary = current_month_data.groupby("部门").agg(
    当月任务量=("本月目标", "sum")
).reset_index() if current_month_data is not None else pd.DataFrame()

month_orders = df_orders_month.groupby("部门").agg(
    当月已开单金额=("已开单金额", "sum"),
    当月已开船金额=("已开船金额", "sum")
).reset_index() if not df_orders_month.empty else pd.DataFrame()

month_summary = month_summary.merge(month_orders, on="部门", how="left").fillna(0)

# 合并累计与当月
final_summary = cum_summary.merge(month_summary, on="部门", how="left").fillna(0)
final_summary["当月开单完成率(%)"] = final_summary.apply(
    lambda r: r["当月已开单金额"] / r["当月任务量"] * 100 if r["当月任务量"] > 0 else 0, axis=1
)

# 使用加权船期完成率（来自 valid_rows）
weighted_by_dept = valid_rows.groupby("部门").agg(
    当月已开单金额=("已开单金额", "sum"),
    当月已开船金额=("已开船金额", "sum")
).reset_index()
weighted_by_dept["当月船期完成率(%)"] = weighted_by_dept["当月已开船金额"] / weighted_by_dept["当月已开单金额"] * 100
final_summary = final_summary.drop(columns=["当月已开船金额", "当月船期完成率(%)"], errors="ignore")
final_summary = final_summary.merge(weighted_by_dept[["部门", "当月船期完成率(%)"]], on="部门", how="left").fillna(0)
cum_sales = df_all.groupby(["部门", "业务员"]).agg(
    累计任务量=("本月目标", "sum"),
    累计已开船金额=("已开船金额", "sum")
).reset_index()
cum_orders_sales = df_orders.groupby(["部门", "业务员"]).agg(
    累计已开单金额=("已开单金额", "sum")
).reset_index()
cum_sales = cum_sales.merge(cum_orders_sales, on=["部门", "业务员"], how="left").fillna(0)
cum_sales["累计船期完成率(%)"] = cum_sales.apply(
    lambda r: r["累计已开船金额"] / r["累计已开单金额"] * 100 if r["累计已开单金额"] > 0 else 0, axis=1)
cum_sales["累计开单完成率(%)"] = cum_sales.apply(
    lambda r: r["累计已开单金额"] / r["累计任务量"] * 100 if r["累计任务量"] > 0 else 0, axis=1)

# 当月有效订单（类目限定 + 开单金额非0）
month_sales_valid = valid_rows.groupby(["部门", "业务员"]).agg(
    当月已开单金额=("已开单金额", "sum"),
    当月已开船金额=("已开船金额", "sum")
).reset_index()
month_sales_valid["当月船期完成率(%)"] = (
    month_sales_valid["当月已开船金额"] / month_sales_valid["当月已开单金额"] * 100
)

month_tasks = current_month_data.groupby(["部门", "业务员"]).agg(
    当月任务量=("本月目标", "sum")
).reset_index()

month_sales_full = month_sales_valid.merge(month_tasks, on=["部门", "业务员"], how="left").fillna(0)
month_sales_full["当月开单完成率(%)"] = (
    month_sales_full["当月已开单金额"] / month_sales_full["当月任务量"] * 100
).replace([np.inf, -np.inf], 0).fillna(0)

# 合并累计 + 当月
final_sales_summary = cum_sales.merge(
    month_sales_full, on=["部门", "业务员"], how="left"
).fillna(0)
# ===== 四部门合计 =====
total_order = valid_rows["已开单金额"].sum()
total_ship = valid_rows["已开船金额"].sum()
overall_completion_rate = total_ship / total_order * 100 if total_order > 0 else 0
# 用 explode() 方式实现层级展开
sales_grouped = final_sales_summary.groupby("部门").agg({"业务员": list}).reset_index()
merged_df = final_summary.merge(sales_grouped, on="部门", how="left")
merged_df = merged_df.explode("业务员")
merged_df["业务员"] = merged_df["业务员"].fillna("【展开查看明细】")
total_row = pd.DataFrame([{
    "部门": "四部门合计",
    "累计任务量": final_summary["累计任务量"].sum(),
    "累计已开单金额": final_summary["累计已开单金额"].sum(),
    "累计已开船金额": final_summary["累计已开船金额"].sum(),
    "累计船期完成率(%)": final_summary["累计已开船金额"].sum() / final_summary["累计已开单金额"].sum() * 100,
    "累计开单完成率(%)": final_summary["累计已开单金额"].sum() / final_summary["累计任务量"].sum() * 100,
    "当月任务量": final_summary["当月任务量"].sum(),
    "当月已开单金额": final_summary["当月已开单金额"].sum(),
    "当月开单完成率(%)": final_summary["当月已开单金额"].sum() / final_summary["当月任务量"].sum() * 100,
    "当月船期完成率(%)": overall_completion_rate
}])
# 展示三个表
print("📊 按部门汇总：")
print(final_summary)

print("📊 按部门 + 业务员汇总：")
print(final_sales_summary)

print("📊 中心合计：")
print(total_row)
