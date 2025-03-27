import pandas as pd
import datetime

# **自动检测当前年份和月份**
current_year = datetime.datetime.now().year
current_month = datetime.datetime.now().month
current_month_str = f"{current_year}.{str(current_month).zfill(2)}"  # 格式 "YYYY.MM"

# **KDocs 在线表格 URL**
book_url = "www.365.kdocs.com/your-file"

try:
    # **读取当前月份工作表**
    df_month = xl(
        range="A1:T1000",
        headers=True,
        sheet_name=current_month_str,
        book_url=book_url
    )

    # **列重命名**
    df_month.columns = [
        "部门", "类目", "业务员", "本月目标", "客户名称", "新连锁开发-区域", 
        "订单号", "订单金额(美金)", "预计开单金额", "已开单金额", "已开船金额", "汇率", 
        "产品交期", "装柜日期", "开船日期", "发货最新进度", "客户型号", "数量", 
        "单价", "备注"
    ]

    # **处理合并单元格问题**
    df_month["类目"] = df_month["类目"].ffill().bfill()

    # **转换数值字段**
    df_month["预计开单金额"] = pd.to_numeric(df_month["预计开单金额"], errors='coerce')

    # **计算在手订单金额（本月预计发货订单 + 已确认订单（明确本月不发货））**
    in_hand_orders = df_month[df_month["类目"].isin(["本月预计发货订单", "已确认订单（明确本月不发货）"])]
    in_hand_order_amount = in_hand_orders["预计开单金额"].sum()

    # **计算洽谈中订单金额（类目 = 洽谈中）**
    negotiation_orders = df_month[df_month["类目"] == "洽谈中"]
    negotiation_order_amount = negotiation_orders["预计开单金额"].sum()

    # **显示结果**
    print(f"📊 {current_month_str} 订单金额统计：")
    print(f"✅ 目前在手订单金额: {in_hand_order_amount:,.2f}")
    print(f"✅ 洽谈中订单金额: {negotiation_order_amount:,.2f}")

except Exception as e:
    print(f"⚠️ 读取 {current_month_str} 失败: {e}")
