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
selected_departments = ["亚太", "亚非", "欧洲事业部", "美洲"]
months = [f"{current_year}.{str(m).zfill(2)}" for m in range(1, current_month + 1)]

all_data = []
current_month_data = None

# 清洗字段
def clean_string_fields(df):
    df["部门"] = df["部门"].astype(str).str.strip()
    df["类目"] = df["类目"].bfill()
    df["业务员"] = df.apply(
    lambda row: row["业务员"]
    if pd.notna(row["业务员"])
    else None if pd.notna(row["订单号"]) else None,
    axis=1
)

# 然后再向前填充空白业务员，但仅针对“非订单新起点”的行
    df["业务员"] = df["业务员"].ffill()
    return df
    

# 正确读取+清洗逻辑
for month in months:
    try:
        # 修复 header 行读取，确保列名正确
    
        df = xl("A1:T1000", headers=True, sheet_name=month, start_row=0, book_url="#WPS在线表格链接#")
        
        # 统一字段命名（避免中英文符号干扰）
        df.columns = [
            "部门", "类目", "业务员", "本月目标", "客户名称", "新连锁开发-区域", "订单号", "订单金额(美金)",
            "预计开单金额", "已开单金额", "已开船金额", "汇率", "发货进度", "发货进度补充说明", "更新时间",
            "产品交期", "装柜日期", "开船日期", "本月可发出概率", "目前完成情况"
        ]

        # 清洗：统一空格、填充类目、修复业务员逻辑
        df["部门"] = df["部门"].astype(str).str.strip()
        df["类目"] = df["类目"].astype(str).str.strip().bfill()

        # 处理“业务员”列（若空，但订单号非空则补为前值）
        df["业务员"] = df.apply(
            lambda row: row["业务员"] if pd.notna(row["业务员"]) else None if pd.notna (row["订单号"]) else None,
            axis=1
        )
        df["业务员"] = df["业务员"].ffill()

        # 仅保留四大目标部门
        df = df[df["部门"].isin(selected_departments)]

        # 数值字段清洗与转化（空字符串或异常替换为0）
        numeric_cols = ["本月目标", "订单金额(美金)", "预计开单金额", "已开单金额", "已开船金额"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # 将“本月目标”从万元转换为元（假设原始单位是万元）
        df["本月目标"] *= 10000

        # 记录当前月数据
        df["月份"] = month
        if month == current_month_str:
            current_month_data = df.copy()

        # 保存至总表
        all_data.append(df)

    except Exception as e:
        print(f"⚠️ {month} 数据读取失败: {e}")



# 合并数据
df_all = pd.concat(all_data, ignore_index=True)

df_orders = df_all[df_all["类目"] == "本月预计发货订单"]

# -------------------------
# 汇总函数
# -------------------------
def build_summary(df_all, df_orders, current_month_data):
    # 所有业务员：总任务量（无论是否开单或开船）
    full_target = df_all.groupby(["部门", "业务员"], as_index=False).agg(
        累计任务量=("本月目标", "sum")
    )

    # 累计开单、开船金额
    cum_orders = df_orders.groupby(["部门", "业务员"], as_index=False).agg(
        累计已开单金额=("已开单金额", "sum"),
        累计已开船金额=("已开船金额", "sum")
    )

    # 合并累计数据
    cum_sales = full_target.merge(cum_orders, on=["部门", "业务员"], how="left").fillna(0)

    # 计算累计完成率
    cum_sales["累计开单完成率(%)"] = cum_sales.apply(
        lambda r: r["累计已开单金额"] / r["累计任务量"] * 100 if r["累计任务量"] > 0 else 0, axis=1
    )
    cum_sales["累计船期完成率(%)"] = cum_sales.apply(
        lambda r: r["累计已开船金额"] / r["累计已开单金额"] * 100 if r["累计已开单金额"] > 0 else 0, axis=1
    )

    # 当月部分：只统计“本月预计发货订单”且“有开单金额”的记录
    df_orders_month = current_month_data[current_month_data["类目"] == "本月预计发货订单"]
    valid_rows = df_orders_month[df_orders_month["已开单金额"] > 0].copy()

    month_sales_valid = valid_rows.groupby(["部门", "业务员"], as_index=False).agg(
        当月已开单金额=("已开单金额", "sum"),
        当月已开船金额=("已开船金额", "sum")
    )
    month_sales_valid["当月船期完成率(%)"] = month_sales_valid.apply(
        lambda r: r["当月已开船金额"] / r["当月已开单金额"] * 100 if r["当月已开单金额"] > 0 else 0, axis=1
    )

    # 获取每位业务员的当月任务量（无论是否有效订单）
    month_tasks = current_month_data.groupby(["部门", "业务员"], as_index=False).agg(
        当月任务量=("本月目标", "sum")
    )

    # 合并月度数据
    month_sales_full = month_tasks.merge(month_sales_valid, on=["部门", "业务员"], how="left").fillna(0)
    month_sales_full["当月开单完成率(%)"] = month_sales_full.apply(
        lambda r: r["当月已开单金额"] / r["当月任务量"] * 100 if r["当月任务量"] > 0 else 0, axis=1
    )

    # 最终结果合并
    final_sales_summary = cum_sales.merge(month_sales_full, on=["部门", "业务员"], how="outer").fillna(0)

    # 金额字段统一转换为万元
    amount_cols = [
        "累计任务量", "累计已开船金额", "累计已开单金额",
        "当月任务量", "当月已开单金额", "当月已开船金额"
    ]
    final_sales_summary[amount_cols] = final_sales_summary[amount_cols] / 10000

    return final_sales_summary

# 构建主表
final_sales_summary = build_summary(df_all, df_orders, current_month_data)

# 部门合计
dept_summary = final_sales_summary.groupby("部门", as_index=False).agg({
    "累计任务量": "sum",
    "累计已开单金额": "sum",
    "累计已开船金额": "sum",
    "当月任务量": "sum",
    "当月已开单金额": "sum",
    "当月已开船金额": "sum"
})
dept_summary["累计船期完成率(%)"] = dept_summary["累计已开船金额"] / dept_summary["累计已开单金额"] * 100
dept_summary["累计开单完成率(%)"] = dept_summary["累计已开单金额"] / dept_summary["累计任务量"] * 100
dept_summary["当月开单完成率(%)"] = dept_summary["当月已开单金额"] / dept_summary["当月任务量"] * 100
dept_summary["当月船期完成率(%)"] = dept_summary["当月已开船金额"] / dept_summary["当月已开单金额"] * 100
dept_summary = dept_summary.fillna(0)
dept_summary["业务员"] = "（部门合计）"

# 合并部门+业务员
final_all = pd.concat([final_sales_summary, dept_summary], ignore_index=True)

# 中心合计

center_base = final_sales_summary.copy()
center_total = center_base.drop(columns=["部门", "业务员"]).sum(numeric_only=True).to_frame().T

center_total["累计船期完成率(%)"] = (
    center_total["累计已开船金额"] / center_total["累计已开单金额"] * 100
    if center_total["累计已开单金额"].values[0] > 0 else 0
)
center_total["累计开单完成率(%)"] = (
    center_total["累计已开单金额"] / center_total["累计任务量"] * 100
    if center_total["累计任务量"].values[0] > 0 else 0
)
center_total["当月开单完成率(%)"] = (
    center_total["当月已开单金额"] / center_total["当月任务量"] * 100
    if center_total["当月任务量"].values[0] > 0 else 0
)
center_total["当月船期完成率(%)"] = (
    center_total["当月已开船金额"] / center_total["当月已开单金额"] * 100
    if center_total["当月已开单金额"].values[0] > 0 else 0
)
center_total["部门"] = "国际营销中心"
center_total["业务员"] = "（中心合计）"

# 合并全部
final_all_with_total = pd.concat([final_all, center_total], ignore_index=True)

# 排序规则
def make_sort_key(row):
    if row["业务员"] == "（中心合计）":
        return "0000"
    elif row["业务员"] == "（部门合计）":
        return f"1_{row['部门']}_00"
    else:
        return f"2_{row['部门']}_{row['业务员']}"

final_all_with_total["排序键"] = final_all_with_total.apply(make_sort_key, axis=1)
final_all_with_total = final_all_with_total.sort_values("排序键").drop(columns="排序键")

# 四舍五入金额 & 比例字段
round_cols = [
    "累计已开船金额", "累计已开单金额", "累计船期完成率(%)", "累计开单完成率(%)",
    "当月已开单金额", "当月已开船金额", "当月船期完成率(%)","当月开单完成率(%)"
]
final_all_with_total[round_cols] = final_all_with_total[round_cols].round(2)
# 输出汇总
print(final_all_with_total)
write_xl(final_all_with_total, "A1:V620",False,"开单+船期")

print("📌 汇总检查：")
print(final_all_with_total[["部门", "业务员", "累计任务量", "累计已开单金额", "累计已开船金额"]].head(10))
