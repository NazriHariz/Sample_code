import polars as pl
import connectorx as cx
from db_connector import get_conn
from datetime import datetime


#GET KPI LIST
tenant = "MT"
start_date = "2024-09-01"
end_date = "2024-09-27"

date_obj = datetime.strptime(start_date, '%Y-%m-%d')
month = date_obj.strftime('%B')

start_time = datetime.now()

def get_kpi_list(tenant):
    FILEHEADER, URI, project_id = get_conn(tenant)
    query = f"SELECT DISTINCT \
                C.CHAIN_NAME, O.OUTLET_ID, O.OUTLET_NAME, GPC.GROUP_CODE, PC.CATEGORY_ID, \
                (SELECT MODEL_ALIAS FROM IR_MODEL WHERE IR_MODEL_ID = PC.IR_MODEL_ID) AS PRODUCT_ALIAS, \
                P.PRODUCT_CODE, P.PRODUCT_NAME, P.IR_LABEL_KEY1, \
                CASE \
                        WHEN LP.SKU_Type = 'Core' THEN 'OSA' \
                        ELSE ISNULL(LP.SKU_Type, '-') \
                END AS SKU_Type \
                from OUTLET_CATEGORY_IR OCI \
                LEFT JOIN LIST_PRODUCT LP ON LP.LIST_ID = OCI.LIST_ID \
                LEFT JOIN PRODUCT P ON P.PRODUCT_ID = LP.PRODUCT_ID \
                LEFT JOIN PRODUCT_CATEGORY PC ON PC.CATEGORY_ID = P.CATEGORY_ID \
                LEFT JOIN IR_Model im ON im.IR_Model_id = pc.IR_Model_id \
                LEFT JOIN OUTLET O ON O.OUTLET_ID = OCI.OUTLET_ID \
                LEFT JOIN CHAIN C ON C.CHAIN_ID = (SELECT CHAIN_ID FROM OUTLET WHERE OUTLET_ID = O.OUTLET_ID)\
                LEFT JOIN Group_Product_Code GPC ON GPC.Product_code = P.Product_code \
                WHERE OCI.ISVALID = 1 \
                AND OCI.AUDIT_MONTH = MONTH('{start_date}') AND OCI.AUDIT_YEAR = YEAR('{start_date}') \
                AND (p.External_code IS NULL OR p.External_code NOT IN \
                        (SELECT \
                                OCES.External_code FROM Outlet_Category_Exemption_SKU OCES \
                                WHERE OCES.Outlet_id = OCI.OUTLET_ID \
                                AND Audit_year = YEAR('{start_date}') \
                                AND Audit_month = MONTH('{start_date}')\
                                AND Category_name = IM.MODEL_ALIAS\
                                AND isValid = 1) \
                )"

    master_kpi_df = cx.read_sql(URI, query, return_type='polars')
    return master_kpi_df

#GET PREDICTED LIST
def get_predicted_list(tenant):
    FILEHEADER, URI, project_id = get_conn(tenant)
    query = f"SELECT DISTINCT\
                O.OUTLET_ID, CC.PROJECT_ID, GPC.GROUP_CODE, CC.ID AS TRX_ID, CC.TRX_DATE AS AUDIT_DATE, \
	            P.PRODUCT_CODE, PC.CATEGORY_ID, \
                CASE \
                    WHEN IM.MODEL_ALIAS IN ('DEODORANTS & FRAGRANCES', 'HNB - DEO') THEN 'DEO'\
                    WHEN IM.MODEL_ALIAS = 'DRESSINGS' THEN 'DRES'\
                    WHEN IM.MODEL_ALIAS IN ('FABRIC CLEANING & ENHANCERS', 'FABRIC CLEANING') THEN 'FAB'\
                    WHEN IM.MODEL_ALIAS = 'FUNCTIONAL NUTRITION' THEN 'BVG'\
                    WHEN IM.MODEL_ALIAS IN ('HAIR CARE', 'HNB - HAIR') THEN 'HR'\
                    WHEN IM.MODEL_ALIAS = 'HOME & HYGIENE' THEN 'HH'\
                    WHEN IM.MODEL_ALIAS = 'ICE CREAM' THEN 'IC'\
                    WHEN IM.MODEL_ALIAS = 'SCRATCH COOKING AIDS' THEN 'SCA'\
                    WHEN IM.MODEL_ALIAS IN ('SKIN CARE', 'HNB - SCR') THEN 'SCR'\
                    WHEN IM.MODEL_ALIAS IN ('SKIN CLEANSING', 'HNB - SCL') THEN 'SCL'\
                    WHEN IM.MODEL_ALIAS IN ('NUTRITIONS', 'OTHER NUTRITION') THEN 'SP'\
                    ELSE IM.MODEL_ALIAS\
                END AS PRODUCT_ALIAS \
                FROM FT_IR_PREDICTIONS PRE \
                LEFT JOIN FT_IR_CC_Photos ph ON ph.Id = pre.FT_IR_Photo_Trx_id \
                LEFT JOIN FT_IR_Category_Checks cc ON cc.Id = ph.Trx_id \
                LEFT JOIN OUTLET O ON O.OUTLET_ID = CC.Outlet_id \
                LEFT JOIN Product p ON (CASE \
                                    WHEN pre.Report_issue = 1 AND pre.Manual_Product_id IS NOT NULL AND pre.Product_id <> pre.Manual_Product_id THEN pre.Manual_Product_id \
                                        ELSE pre.Product_id \
                                    END) = p.Product_id \
                LEFT JOIN Product_Category pc ON pc.Category_id = p.Category_id \
                LEFT JOIN IR_Model im ON im.IR_Model_id = pc.IR_Model_id \
                LEFT JOIN Group_Product_Code gpc ON GPC.Product_code = P.Product_code \
                WHERE CC.TRX_DATE >= '{start_date}' AND CC.TRX_DATE <= '{end_date}' AND CC.PROJECT_ID = {project_id}\
                AND pre.isValid = 1 \
                AND ISNUMERIC(im.Min_def_confidence) = 1 \
                AND CAST( \
                    CASE WHEN pre.Type = 'Manual' THEN pre.Manual_Confidence ELSE pre.Confidence END AS FLOAT \
                ) >= CAST(im.Min_def_confidence AS FLOAT)"


    predicted_df = cx.read_sql(URI, query, return_type='polars')
    return predicted_df

df_kpi = get_kpi_list(tenant)
df_predicted = get_predicted_list(tenant)


df_kpi = df_kpi.with_columns([
    pl.lit("").alias("STATUS"),
    pl.lit("").alias("AUDIT_STATUS"),
    pl.concat_str([df_kpi["OUTLET_ID"], df_kpi["GROUP_CODE"].fill_null(""), df_kpi["PRODUCT_CODE"]]).alias("key_with_group"),
    pl.concat_str([df_kpi["OUTLET_ID"], df_kpi["PRODUCT_CODE"]]).alias("key_without_group"),
    pl.concat_str([df_kpi["OUTLET_ID"], df_kpi["CATEGORY_ID"]]).alias("key_with_group_auditcheck")
])

df_predicted = df_predicted.with_columns([
    pl.concat_str([df_predicted["OUTLET_ID"], df_predicted["GROUP_CODE"].fill_null(""), df_predicted["PRODUCT_CODE"]]).alias("key_with_group"),
    pl.concat_str([df_predicted["OUTLET_ID"], df_predicted["PRODUCT_CODE"]]).alias("key_without_group"),
    pl.concat_str([df_predicted["OUTLET_ID"], df_predicted["CATEGORY_ID"]]).alias("key_with_group_auditcheck"),
    pl.concat_str([
                pl.lit("https://ulm.retail-aim.com/QuickView/Pages/ViewIRResultV2.aspx?irID="),
                pl.col("TRX_ID"),
                pl.lit("&ProjID="),
                pl.col("PROJECT_ID"),
                pl.lit("&Category="),
                pl.col("PRODUCT_ALIAS")
            ], separator="").alias("QV_Link")
])

df_kpi = df_kpi.with_columns(
    pl.when(pl.col("PRODUCT_ALIAS").is_in(["DEODORANTS & FRAGRANCES", "HNB - DEO"]))
    .then(pl.lit("DEO"))
    .when(pl.col("PRODUCT_ALIAS") == "DRESSINGS")
    .then(pl.lit("DRES"))
    .when(pl.col("PRODUCT_ALIAS").is_in(["FABRIC CLEANING & ENHANCERS", "FABRIC CLEANING"]))
    .then(pl.lit("FAB"))
    .when(pl.col("PRODUCT_ALIAS") == "FUNCTIONAL NUTRITION")
    .then(pl.lit("BVG"))
    .when(pl.col("PRODUCT_ALIAS").is_in(["HAIR CARE", "HNB - HAIR"]))
    .then(pl.lit("HR"))
    .when(pl.col("PRODUCT_ALIAS") == "HOME & HYGIENE")
    .then(pl.lit("HH"))
    .when(pl.col("PRODUCT_ALIAS") == "ICE CREAM")
    .then(pl.lit("IC"))
    .when(pl.col("PRODUCT_ALIAS") == "SCRATCH COOKING AIDS")
    .then(pl.lit("SCA"))
    .when(pl.col("PRODUCT_ALIAS").is_in(["SKIN CARE", "HNB - SCR"]))
    .then(pl.lit("SCR"))
    .when(pl.col("PRODUCT_ALIAS").is_in(["SKIN CLEANSING", "HNB - SCL"]))
    .then(pl.lit("SCL"))
    .when(pl.col("PRODUCT_ALIAS").is_in(["NUTRITIONS", "OTHER NUTRITION"]))
    .then(pl.lit("SP"))
    .when(pl.col("PRODUCT_ALIAS") == "FUNCTIONAL NUTRITION")
    .then(pl.lit("BVG"))
    .otherwise(pl.col("PRODUCT_ALIAS"))  # Retain original value if no conditions match
    .alias("PRODUCT_ALIAS")  # Rename or replace the existing column
)

# Update STATUS column based on the existence of either key in df_predicted
df_kpi = df_kpi.with_columns(
    pl.when(
        df_kpi["key_with_group"].is_in(df_predicted["key_with_group"]) |
        df_kpi["key_without_group"].is_in(df_predicted["key_without_group"])
    ).then(pl.lit("PRESENT")).otherwise(pl.lit("NOT PRESENT")).alias("STATUS")
)
df_kpi = df_kpi.with_columns(
    pl.when(
        df_kpi["key_with_group_auditcheck"].is_in(df_predicted["key_with_group_auditcheck"])
    ).then(pl.lit("Audited")).otherwise(pl.lit("Non-Audited")).alias("AUDIT_STATUS")
)

# Set of unique keys for audit check from df_predicted
df_kpi = df_kpi.select(pl.exclude(["key_with_group", "key_without_group", "key_with_group_auditcheck"]))
df_kpi = df_kpi.filter(pl.col("AUDIT_STATUS") == pl.lit("Audited")) #filtered to audit only

# Aggregate predicted DataFrame to ensure unique combinations
predicted_unique = df_predicted.group_by(["OUTLET_ID", "CATEGORY_ID"]).agg([
    pl.col("AUDIT_DATE").first(),  # or another aggregation if appropriate
    pl.col("TRX_ID").first(),
    pl.col("QV_Link").first()
])

#to map qv link with trx_id
final_df = df_kpi.join(predicted_unique,on=["OUTLET_ID", "CATEGORY_ID"], how="left")

end_time = datetime.now()

execution_time = end_time - start_time
print(execution_time)
print(final_df)

# file_path = rf"/home/ir-nazri/Documents/TEST-SEPTEMBER5.csv"
#
# final_df.write_csv(file_path)

# print(f"CSV Generate Succesfully at {file_path}")