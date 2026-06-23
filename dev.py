import sqlglot
from sqlglot import exp
from sqlglot import parse_one
from sqlglot.optimizer import qualify
from sqlglot.lineage import lineage

sql = """
with dt as (select date(min(fildate)) start_date from ods.hd_buy1s where rcvtime >= '2026-04-01')

select 
		t.sales_date,
		t.store_id,
		t6.year_number as sales_year,
		t6.month_number as sales_month,
		t6.week_of_year as sales_week_num,
		t6.day_of_week_cn as sales_weekday,
		t2.store_code,
		t2.store_name,
		t2.province_name,
		t2.city_name,
		t2.store_category,
		t2.region_code1,
		t2.region_name1,
		t2.region_code2,
		t2.region_name2,
		t.sales_qty,
		t.sales_amt,
		t.sales_cost,
		t.retail_sales_amt as total_amt,
		ifnull(t.trans_cnt,0)-ifnull(t4.invalid_trans_cnt,0) as transaction_cnt,
		ifnull(t3.passenger_flow_cnt,0) as customer_flow,
		ifnull(t.member_trans_cnt,0)-ifnull(t4.invalid_trans_cnt,0) as member_trans_cnt,
		t.member_sales_qty,
		t.member_sales_amt,
		t.member_sales_cost,
		t.member_retail_sales_amt as member_total_amt,
		ifnull(t5.new_first_pur_member_cnt,0) as new_member_cnt,
		if(t.trans_cnt-ifnull(t4.invalid_trans_cnt,0)=0,0, sales_amt/(t.trans_cnt-ifnull(t4.invalid_trans_cnt,0))) as avg_order_amt,
		if(sales_qty=0,0, sales_amt/sales_qty) as avg_item_price,
		if(t.trans_cnt-t.member_trans_cnt+ifnull(t5.new_first_pur_member_cnt,0)=0,0, ifnull(t5.new_first_pur_member_cnt,0)/(t.trans_cnt-t.member_trans_cnt+ifnull(t5.new_first_pur_member_cnt,0)))*100 as member_conversion_rate,
		if(sales_amt=0,0, member_sales_amt/sales_amt)*100 as member_contribution_rate,
		if(ifnull(t3.passenger_flow_cnt,0)=0,0, (ifnull(t.trans_cnt,0)-ifnull(t4.invalid_trans_cnt,0))/ifnull(t3.passenger_flow_cnt,0))*100 as transaction_success_rate
from(
		select 
				order_date as sales_date,
				store_id,
				sum(sales_qty) as sales_qty,
				sum(sales_amt ) as sales_amt,
				sum(ifnull(cost_amt,0)+ifnull(tax_amt,0)) sales_cost,
				sum(retail_sales_amt) as retail_sales_amt,
				count(distinct order_id ) as trans_cnt,
				count( distinct case when member_id is not null then order_id end ) as member_trans_cnt,
				sum( case when member_id is not null then sales_qty end ) as member_sales_qty,
				sum( case when member_id is not null then sales_amt end ) as member_sales_amt,
				sum( case when member_id is not null then ifnull(cost_amt,0)+ifnull(tax_amt,0) end ) as member_sales_cost,
				sum( case when member_id is not null then retail_sales_amt end ) as member_retail_sales_amt
		from dwd.pos_order_goods
		where order_date >= '2026-04-01'
		and order_date < '2026-04-02'
		and store_code not in ('1001','10001')
		group by order_date,store_id
)t
inner join (
		select
		s.gid AS store_id,
		s.code AS store_code,
		s.name AS store_name,
		s.engshortdesc AS store_category,
		s.province AS province_name,
		s.city AS city_name,
		h.acode AS region_code1,
		h.aname AS region_name1,
		h.bcode AS region_code2,
		h.bname AS region_name2
	from ods.hd_store s
	left join ods.hd_v_area_store h on s.gid = h.storegid
)t2 on t.store_id=t2.store_id
left join(
    select event_date,store_id,sum(passenger_flow_cnt) passenger_flow_cnt
    from dwd.traffic_store_detail
    where 1 = 1
    and event_date >= (select max(start_date) from dt)
    and event_date < '2026-04-02'
    and hour(event_time) BETWEEN 8 and 24
    group by event_date,store_id
)t3 on t.sales_date=t3.event_date and t.store_id = t3.store_id
left join ads.store_sales_daily_invalid t4 on t.sales_date =t4.sales_date and t.store_id=t4.store_id
left join (
    select first_sale_date as sales_date,first_sale_store_id as store_id,count(1) as new_first_pur_member_cnt
    from dwd.dim_member m
    where  NOT EXISTS ( SELECT 1 FROM ods.hd_ludao_sp_member sm WHERE m.member_id = sm.memberid ) 
		and first_sale_date >= (select max(start_date) from dt)
    and first_sale_date < '2026-04-02'
    group by first_sale_date,first_sale_store_id
)t5 on t.sales_date =t5.sales_date and t.store_id=t5.store_id
inner join dwd.dim_date t6 on t.sales_date=t6.full_date
"""




schema_mapping = {

"dwd.dim_date":{"full_date":"date","date_id":"int","week_id":"int","month_id":"int","date_cn":"string","date_short":"int","day_of_week":"int","day_of_week_cn":"string","day_of_month":"int","day_of_year":"int","week_of_year":"int","week_start_date":"date","week_end_date":"date","month_number":"int","month_number_cn":"string","month_name":"string","month_short_name":"string","month_start_date":"date","month_end_date":"date","quarter_number":"int","quarter_name":"string","quarter_start_date":"date","quarter_end_date":"date","year_number":"int","year_start_date":"date","year_end_date":"date","is_workday":"int"},
"dwd.dim_member":{"member_id":"string","name":"string","nickname":"string","grade":"string","grade_type":"string","payment_time":"datetime","state":"string","register_channel_type":"string","register_time":"datetime","creator_name":"string","create_time":"datetime","last_modifier_id":"string","last_modified_time":"datetime","first_sale_date":"date","first_sale_time":"datetime","first_sale_store_id":"int","last_sale_date":"date","last_sale_time":"datetime","last_sale_store_id":"int","total_sale_qty":"float","total_sale_amt":"float","first_sale_amt":"float","last_sale_amt":"float","unit_price":"float","max_unit_price":"float","master_member_id":"string","old_member_id":"string","first_order_no":"string","last_order_no":"string","etl_time":"datetime"},
"dwd.pos_order_goods":{"order_id":"string","line_no":"int","order_date":"date","order_time":"datetime","store_id":"int","store_code":"string","goods_id":"int","member_id":"string","cashier_id":"int","card_no":"string","retail_price":"float","member_price":"float","discount_price":"float","screen_price":"float","sales_qty":"float","sales_amt":"float","retail_sales_amt":"float","screen_sales_amt":"float","sales_cost":"float","cost_amt":"float","discount_amt":"float","tax_amt":"float","erp_time":"datetime","tax_rate":"float","etl_time":"datetime"},
"dwd.traffic_store_detail":{"event_time":"datetime","store_id":"int","event_date":"date","passenger_flow_cnt":"int","etl_time":"datetime"},
"ods.hd_buy1s":{"posno":"string","flowno":"string","fildate":"datetime","cashier":"int","stdtotal":"float","scrtotal":"float","realamt":"float","cardno":"string","reccnt":"int","memo":"string","scrfavmode":"int","scrfavvalue":"float","dealer":"int","score":"float","rcvtime":"datetime","classdate":"datetime","classno":"int","busdate":"datetime","continuousflowno":"string","vouchercode":"string","vouchertype":"int","servicetype":"string","srcflowno":"string","iskillallsale":"int","currencygid":"int","exgrate":"float","srcstdtotal":"float","srcscrtotal":"float","srcrealamt":"float","srcscrfavvalue":"float","takeoutfoodordno":"string","memberid":"string","uuid":"string","terminal":"string","exgratetype":"int","flag":"int","payno":"string","srcposno":"string","busitype":"int","taxno":"string","eleinvoicecode":"string","srccls":"string","datasrc":"int","invoiceno":"string","customerinfo":"string","unionpayflowno":"string","bckreason":"string","foodprocstat":"int","assistant":"int","remark":"string","flownoflag":"int","tokentotal":"float","membergrade":"string","customerselfpay":"int","relatedflowno":"string","buytype":"int","businesstags":"string","retstoreno":"string","retposno":"string","realretstoreno":"string","realretposno":"string","membercode":"string","memberphone":"string","memberidentifytype":"int","featurecode":"string","invoicereservationcode":"string","invoicetranid":"string","assistantstore":"int","taxfree":"int","dwmabusdate":"datetime","etl_time":"datetime"},



}

# 1. 解析 SQL
expression = sqlglot.parse_one(sql, read='doris')

sql2 = qualify.qualify(
    expression,
    schema=schema_mapping,
    dialect="doris"
).sql()

expression = sqlglot.parse_one(sql2, read='doris')

# 2. 提取所有 CTE 的别名（即临时表名）
cte_names = {cte.alias for cte in expression.find_all(exp.CTE)}

# 3. 提取物理表和字段
source_tables = set()
source_columns = set()

# 遍历所有表节点
for table in expression.find_all(exp.Table):
    # 如果表名不在 CTE 列表中，且不是目标表（这里简单通过逻辑判断，实际需结合 Insert 节点分析）
    if table.name not in cte_names:
        # 排除目标表逻辑：在 INSERT INTO 语句中，第一个表通常是目标表
        # 这里为了演示简单，我们假设所有非 CTE 表都是源表（实际生产中需更严谨判断 INSERT/UPDATE 目标）
        source_tables.add(table.name)

        # 获取库名
        db_name = table.db if table.db else "default_db"
        print(f"📊 发现物理表: {db_name}.{table.name}")

# 4. 提取所有涉及的字段
for column in expression.find_all(exp.Column):
    # 获取字段名和它所属的表（别名）
    col_name = column.name
    table_alias = column.table
    source_columns.add(f"{table_alias}.{col_name}")


    print(f"\n📝 涉及的源字段: {col_name}")







print("🔍 提取到的字段依赖：")
for column in expression.find_all(exp.Column):
    print(column)
    # column.name 是字段名
    # column.table 是表别名或表名
    print(f"- 字段: {column.name}, 来源表/别名: {column.table}")

    # 如果需要判断是否为物理表字段，可以检查 table 是否在 cte_names 中
    if column.table not in cte_names:
        print(f"  -> 这是一个物理表字段")
    else:
        print(f"  -> 这是一个 CTE 临时表字段")

    lineage_node = lineage(column=column, sql=sql)

    print(lineage_node)