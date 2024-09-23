import module.Util as Util
import module.SqlManager as SqlManager
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta



async def GetTotalList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 3:
		return 400, "invalid permission", {}
	
	
	stock_sql_query_str = """
		SELECT 'Stock' AS table_type, stock_code, stock_name_kr
		FROM KoreaInvest.stock_info
		ORDER BY stock_code
		WHERE stock_update > NOW() - INTERVAL 2 WEEK
	"""
	coin_sql_query_str = """
		SELECT 'Coin' AS table_type, coin_code, coin_name_kr
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

	return 200, "success", { "list" : last_query_list }
	

async def GetRegistedQueryList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 3:
		return 400, "invalid permission", {}
	

	stock_sql_query_str = """
		SELECT 'Stock' AS table_type, Q.stock_code, Q.query_type, I.stock_name_kr
		FROM KoreaInvest.stock_last_ws_query AS Q
		JOIN KoreaInvest.stock_info AS I
		ON Q.stock_code = I.stock_code
	"""
	coin_sql_query_str = """
		SELECT 'Coin' AS table_type, Q.coin_code, Q.query_type, I.coin_name_kr
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

	return 200, "success", { "list" : last_query_list }


async def UpdateRegistedQueryList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 1:
		return 400, "invalid permission", {}
	

	target_info_list = req_dict["list"]	# table_type, target_code, query_type

	stock_insert_list = []
	code_type_dict = {target_info[1]: target_info[2] for target_info in target_info_list if target_info[0] != "COIN"}
	if len(code_type_dict) > 0:
		code_list = list(code_type_dict.keys())
		code_strings = ','.join(['%s'] * len(code_list))
		query_str = f"""
			SELECT stock_code, stock_market
			FROM KoreaInvest.stock_info
			WHERE stock_code IN ({code_strings}) AND stock_update > NOW() - INTERVAL 2 WEEK
		"""
		query_ret_list = await SqlManager.sql_manager.Get(query_str)

		for query_ret in query_ret_list:
			code = query_ret[0]
			query_type = code_type_dict[code]
			market = query_ret[1]

			if query_type == "EX":			# 지금은 체결내용만 받기로함
				if market == "KOSPI" or market == "KOSDAQ" or market == "KONEX":
					api_code = "H0STCNT0"
					api_prefix = ""
				elif market == "NYSE":
					api_code = "HDFSCNT0"
					api_prefix = "DNYS"
				elif market == "NASDAQ":
					api_code = "HDFSCNT0"
					api_prefix = "DNAS"
				elif market == "AMEX":
					api_code = "HDFSCNT0"
					api_prefix = "DAMS"
				else:
					return 500, "invalid market detected", {}
				
				stock_insert_list.append((
					f"{query_type}_{code}",
					code,
					query_type,
					api_code,
					f"{api_prefix}{api_code}",
				))

			elif query_type == "OD":
				# 아직 준비 안되어 있어서 주석처리
				return 500, "OD type is not available", {}

				if market == "KOSPI" or market == "KOSDAQ" or market == "KONEX":
					api_code = "H0STASP0"
					api_prefix = ""
				elif market == "NYSE":
					api_code = "HDFSASP0"
					api_prefix = "DNYS"
				elif market == "NASDAQ":
					api_code = "HDFSASP0"
					api_prefix = "DNAS"
				elif market == "AMEX":
					api_code = "HDFSASP0"
					api_prefix = "DAMS"
				else:
					return 500, "invalid market detected", {}
				
				stock_insert_list.append((
					f"{query_type}_{code}",
					code,
					query_type,
					api_code,
					f"{api_prefix}{api_code}",
				))
				
			else:
				return 400, "invalid query_type detected", {}

	coin_insert_list = []
	code_type_dict = {target_info[1]: target_info[2] for target_info in target_info_list if target_info[0] == "COIN"}
	if len(code_type_dict) > 0:
		code_list = list(code_type_dict.keys())
		code_strings = ','.join(['%s'] * len(code_list))
		query_str = f"""
			SELECT coin_code
			FROM Bithumb.coin_info
			WHERE coin_code IN ({code_strings}) AND coin_update > NOW() - INTERVAL 2 WEEK
		"""
		query_ret_list = await SqlManager.sql_manager.Get(query_str)

		for query_ret in query_ret_list:
			code = query_ret[0]
			query_type = code_type_dict[code]

			if query_type == "EX":			# 지금은 체결내용만 받기로함
				coin_insert_list.append((
					f"{query_type}_{code}",
					code,
					query_type,
					"transaction",
					f"{api_code}_KRW",
				))

			elif query_type == "OD":
				# 아직 준비 안되어 있어서 주석처리
				return 500, "OD type is not available", {}
							
				coin_insert_list.append((
					f"{query_type}_{code}",
					code,
					query_type,
					"orderbooksnapshot",
					f"{api_code}_KRW",
				))
				
			else:
				return 400, "invalid query_type detected", {}


	query_str_list = [
		"DELETE FROM KoreaInvest.stock_last_ws_query",
		"DELETE FROM Bithumb.coin_last_ws_query",
	]
	if len(stock_insert_list) > 0:
		values_str = ', '.join([f"({', '.join(map(str, record))})" for record in stock_insert_list])
		query_str_list.append(f"""INSERT INTO KoreaInvest.stock_last_ws_query VALUES {values_str}""")
	if len(coin_insert_list) > 0:
		values_str = ', '.join([f"({', '.join(map(str, record))})" for record in coin_insert_list])
		query_str_list.append(f"""INSERT INTO Bithumb.coin_last_ws_query VALUES {values_str}""")

	is_good = await SqlManager.sql_manager.Set(query_str_list)
	if is_good:
		return 200, "success", {}
	else:
		return 500, "fail to update", {}


async def GetCandleData(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 3:
		return 400, "invalid permission", {}
	
	table_type = req_dict["table_type"]
	target_code = req_dict["target_code"].replace("/", "_")
	year, week_num, week_day  = DateTime.now().isocalendar()
	week_from = week_num
	week_to = week_num
	if "yaer" in req_dict and "week_from" in req_dict and "week_to" in req_dict:
		year = req_dict["year"]
		week_from = req_dict["week_from"]
		week_to = req_dict["week_to"]

	query_str = f"""
		SELECT *
		FROM Z_{table_type}{target_code}.Candle{year}
		WHERE YEARWEEK(execution_datetime, 0)
		BETWEEN CONCAT({year}, LPAD({week_from}, 2, '0')) AND CONCAT({year}, LPAD({week_to}, 2, '0'));
	"""

	candle_data = await SqlManager.sql_manager.Get(query_str)
	
	return 200, "success", { "candle" : candle_data }

	

