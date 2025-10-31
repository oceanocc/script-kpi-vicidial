from dotenv import load_dotenv
import mysql
import os

# Load .env variables
load_dotenv()

# Variables
host = os.getenv('host')
user = os.getenv('user')
passwd = os.getenv('passwd')
db = os.getenv('db')
port = os.getenv('port')

db = mysql.connector.connect(host=host, user=user, passwd=passwd, db=db, port=port)
cursor = db.cursor()

query = f"""
    INSERT INTO kpi_listas_vicidial (list_id,lista,llamadas,transferidas,porc_transferidas,ventas_vicidial,efectividad,txv,ov1m,ov2m,ov3m,ov4m,ov5m,fecha)
    SELECT
        hvd.list_id AS 'ID de Lista'
        ,vl.list_name 'Nombre de lista'
        ,COUNT(hvd.phone_number) AS 'Llamadas'
        ,SUM(IF(vcs.human_answered = 'Y', 1, 0)) AS 'Transferidas'
        ,ROUND(SUM(IF(vcs.human_answered = 'Y', 1, 0)) / NULLIF(COUNT(hvd.phone_number), 0)* 100, 2) AS '% Transferidas'
        ,SUM(IF(vcs.sale = 'Y', 1, 0)) AS 'Ventas Vicidial'
        ,ROUND(SUM(IF(vcs.sale = 'Y', 1, 0)) / NULLIF(SUM(IF(vcs.human_answered = 'Y', 1, 0)), 0) * 100, 2) AS 'Efectividad'
        ,ROUND(SUM(IF(vcs.human_answered = 'Y', 1, 0)) / NULLIF(SUM(IF(vcs.sale = 'Y', 1, 0)), 0), 2) AS 'TxV'
        ,SUM(IF(hvd.length_in_sec >= 60 AND hvd.length_in_sec < 120, 1, 0)) AS 'OV1M'
        ,SUM(IF(hvd.length_in_sec >= 120 AND hvd.length_in_sec < 180, 1, 0)) AS 'OV2M'
        ,SUM(IF(hvd.length_in_sec >= 180 AND hvd.length_in_sec < 240, 1, 0)) AS 'OV3M'
        ,SUM(IF(hvd.length_in_sec >= 240 AND hvd.length_in_sec < 300, 1, 0)) AS 'OV4M'
        ,SUM(IF(hvd.length_in_sec >= 300, 1, 0)) AS 'OV5M'
        ,DATE(hvd.call_date)
    FROM asterisk.vicidial_log_archive hvd
    LEFT JOIN asterisk.vicidial_lists vl ON vl.list_id = hvd.list_id
    LEFT JOIN asterisk.vicidial_campaign_statuses vcs ON vcs.status = hvd.status AND vcs.campaign_id = hvd.campaign_id
    WHERE
        DATE(hvd.call_date) = DATE(NOW() - INTERVAL 1 DAY)
    GROUP BY DATE(hvd.call_date), hvd.list_id
"""
cursor.execute(query)
cursor.commit()