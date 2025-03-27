# 重新加载环境并执行脚本（包含二级指标“类型”分类）
import pandas as pd
import datetime

pd.set_option('display.float_format', '{:,.2f}'.format)
pd.set_option('display.max_rows',None)
# 当前年月
current_year = datetime.datetime.now().year
current_month = datetime.datetime.now().month
months = [f"{current_year}.{str(m).zfill(2)}" for m in range(1, current_month + 1)]
current_month_str = f"{current_year}.{str(current_month).zfill(2)}"

# 二级指标字段映射
valid_indicators = [
    "ODM开发+增量", "经销商开发-重点空白", "经销商增量-占比低", "经销商增量-占比高",
    "其他", "现有连锁增量", "新连锁开发-区域", "新连锁开发-全球",
    "区域新连锁开发", "ODM增量", "经销商开发", "ODM开发", "全球新连锁开发"
]

# 二级指标 → 类型 映射
indicator_type_map = {
    "新连锁开发-区域": "拓展",
    "ODM开发+增量": "固本",
    "区域新连锁开发": "拓展",
    "经销商开发-重点空白": "拓展",
    "其他": "其他",
    "经销商增量-占比低": "固本",
    "ODM增量": "固本",
    "新连锁开发-全球": "拓展",
    "现有连锁增量": "固本",
    "经销商增量-占比高": "固本",
    "经销商开发": "拓展",
    "ODM开发": "拓展",
    "全球新连锁开发": "拓展"
}

all_data = []
current_month_data = None

# 读取每月工作表
for month in months:
    try:
        df = xl("A1:T1000", headers=True, sheet_name=month,
                book_url="www.365.kdocs.com/your-file")
        df.columns = [
            "部门", "类目", "业务员", "本月目标", "客户名称", "二级指标",
            "订单号", "订单金额(美金)", "预计开单金额", "已开单金额", "已开船金额", "汇率",
            "产品交期", "装柜日期", "开船日期", "发货最新进度", "客户型号", "数量",
            "单价", "备注"
        ]
        df["月份"] = month
        df["类目"] = df["类目"].bfill()
        df["二级指标"] = df["二级指标"].fillna("其他")  # 防止 NaN
        df["类型"] = df["二级指标"].map(indicator_type_map).fillna("其他")
        df = df[df["二级指标"].isin(valid_indicators)]
        
        for col in ["本月目标", "预计开单金额", "已开单金额", "已开船金额"]:
            df[col] = pd.to_numeric(df[col].replace(r"^\s*$", "", regex=True), errors="coerce").fillna(0)

        if not df.empty:
            all_data.append(df)
        else:
            print(f"⚠️ {month} 数据为空，未加入 all_data")

        if month == current_month_str:
            current_month_data = df.copy()

    except Exception as e:
        print(f"⚠️ {month} 加载失败: {e}")

df_all = pd.concat(all_data, ignore_index=True)

# ========== 累计数据 ==========
summary_cum = df_all.groupby(["二级指标", "类型"]).agg(
    累计已开单金额=("已开单金额", "sum"),
).reset_index()

# 计算累计占比
total_cum_order = summary_cum["累计已开单金额"].sum()
summary_cum["累计开单占比(%)"] = (
    summary_cum["累计已开单金额"] / total_cum_order * 100
).fillna(0) if total_cum_order > 0 else 0

# ========== 当月数据 ==========
df_month = current_month_data[current_month_data["二级指标"].isin(valid_indicators)].copy()
summary_month = df_month.groupby(["二级指标", "类型"]).agg(
    当月已开单金额=("已开单金额", "sum")
).reset_index()

# 计算当月占比
total_month_order = summary_month["当月已开单金额"].sum()
summary_month["当月开单占比(%)"] = (
    summary_month["当月已开单金额"] / total_month_order * 100
).fillna(0) if total_month_order > 0 else 0

# 合并累计 + 当月数据
summary = summary_cum.merge(summary_month, on=["二级指标", "类型"], how="outer").fillna(0)

# ========== 部门数据 ==========
detail_region = df_all.groupby(["二级指标", "类型", "部门"]).agg(
    累计已开单金额=("已开单金额", "sum"),
).reset_index()

# 计算部门的占比
detail_region["累计开单占比(%)"] = detail_region.groupby(["类型", "部门"])["累计已开单金额"].transform(lambda x: (x / x.sum() * 100).fillna(0))

# ========== 业务员数据 ==========
detail_sales = df_all.groupby(["二级指标", "类型", "部门", "业务员"]).agg(
    累计已开单金额=("已开单金额", "sum"),
).reset_index()

# 计算业务员的占比
detail_sales["累计开单占比(%)"] = detail_sales.groupby(["类型", "部门", "业务员"])["累计已开单金额"].transform(lambda x: (x / x.sum() * 100).fillna(0))

# ========== 展示 ==========
print("二级指标总结")
print(summary)
print(detail_region)
print(detail_sales)
