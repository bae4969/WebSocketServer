import module.Util as Util
import module.SqlManager as SqlManager



async def GetRegistedQueryList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	stock_sql_query_str = """
		SELECT 'stock' AS table_type, Q.stock_code, Q.query_type, I.stock_name_kr
		FROM KoreaInvest.stock_last_ws_query AS Q
		JOIN KoreaInvest.stock_info AS I
		ON Q.stock_code = I.stock_code
	"""
	coin_sql_query_str = """
		SELECT 'coin' AS table_type, Q.coin_code, I.coin_name_kr
		FROM Bithumb.coin_last_ws_query AS Q
		JOIN Bithumb.coin_info AS I
		ON Q.coin_code = I.coin_code
	"""
	
	region = req_dict["region"]
	if region == "KR":
		query_str = stock_sql_query_str + " WHERE I.stock_market='KOSPI' OR I.stock_market='KOSDAQ' OR I.stock_market='KONEX'"
	elif region == "US":
		query_str = stock_sql_query_str + " WHERE I.stock_market='NYSE' OR I.stock_market='NASDAQ' OR I.stock_market='AMEX'"
	elif region == "COIN":
		query_str = coin_sql_query_str
	else:
		query_str = stock_sql_query_str + " UNION " + coin_sql_query_str

	last_query_list = await SqlManager.sql_manager.Get(query_str)

	return 0, "success", { "list" : last_query_list }


async def GetTotalValidList(client_info:dict, req_dict:dict):
	stock_sql_query_str = """
		SELECT 'stock' AS table_type, stock_code, stock_name_kr
		FROM KoreaInvest.stock_info
		ORDER BY stock_code
		WHERE stock_update > NOW() - INTERVAL 2 WEEK
	"""
	coin_sql_query_str = """
		SELECT 'coin' AS table_type, coin_code, coin_name_kr
		FROM Bithumb.coin_info
		ORDER BY coin_code
		WHERE coin_update > NOW() - INTERVAL 2 WEEK
	"""

	region = req_dict["region"]
	if region == "KR":
		query_str = stock_sql_query_str + " AND stock_market='KOSPI' OR stock_market='KOSDAQ' OR stock_market='KONEX'"
	elif region == "US":
		query_str = stock_sql_query_str + " AND stock_market='NYSE' OR stock_market='NASDAQ' OR stock_market='AMEX'"
	elif region == "COIN":
		query_str = coin_sql_query_str
	else:
		query_str = stock_sql_query_str + " UNION " + coin_sql_query_str

	offset = req_dict["offset"]
	query_str += f" LIMIT 100 OFFSET {(offset - 1) * 100}"

	last_query_list = await SqlManager.sql_manager.Get(query_str)

	return 0, "success", { "list" : last_query_list }
	
