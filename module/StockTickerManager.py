import module.Util as Util
import module.SqlManager as SqlManager
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta



async def GetTotalList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 3:
		return 400, "invalid permission", {}
	
	
	stock_sql_query_str = f"""
		SELECT 'STOCK' AS table_type, stock_code, stock_name_kr, stock_market
		FROM KoreaInvest.stock_info
		WHERE stock_update > NOW() - INTERVAL 2 WEEK
	"""
	coin_sql_query_str = f"""
		SELECT 'COIN' AS table_type, coin_code, coin_name_kr, 'COIN' AS stock_market
		FROM Bithumb.coin_info
		WHERE coin_update > NOW() - INTERVAL 2 WEEK
	"""

	region = Util.TryGetDictStr(req_dict, "stock_region", "")
	type = Util.TryGetDictStr(req_dict, "stock_type", "")
	offset = Util.TryGetDictInt(req_dict, "list_offset", 0)

	if type == "STOCK":
		stock_sql_query_str += " AND stock_type='STOCK'"
	elif type == "ETF":
		stock_sql_query_str += " AND stock_type='ETF'"
	elif type == "ETN":
		stock_sql_query_str += " AND stock_type='ETN'"

	if region == "KR":
		query_str = stock_sql_query_str + " AND (stock_market='KOSPI' OR stock_market='KOSDAQ' OR stock_market='KONEX')"
	elif region == "US":
		query_str = stock_sql_query_str + " AND (stock_market='NYSE' OR stock_market='NASDAQ' OR stock_market='AMEX')"
	elif region == "COIN":
		query_str = coin_sql_query_str
	else:
		query_str = stock_sql_query_str + " UNION " + coin_sql_query_str

	query_str += f" ORDER BY stock_code LIMIT 100 OFFSET {offset * 100}"

	sql_code, sql_data = await SqlManager.sql_manager.Get(query_str)
	if sql_code == 0:
		return 200, "success", { "list" : sql_data }
	else:
		return 500, "fail to get data", { "list" : sql_data }
	
	
async def SearchTotalList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 3:
		return 400, "invalid permission", {}
	
	search_str = req_dict["search_keyword"].replace("'", "")
	if len(search_str) < 2:
		return 400, "keyword must be longer than 1", {}
	
	stock_sql_query_str = f"""
		SELECT 'STOCK' AS table_type, stock_code, stock_name_kr, stock_market
		FROM KoreaInvest.stock_info
		WHERE stock_update > NOW() - INTERVAL 2 WEEK AND (stock_code LIKE '%{search_str}%' OR stock_name_kr LIKE '%{search_str}%' OR stock_name_en LIKE '%{search_str}%')
	"""
	coin_sql_query_str = f"""
		SELECT 'COIN' AS table_type, coin_code, coin_name_kr, 'COIN' AS stock_market
		FROM Bithumb.coin_info
		WHERE coin_update > NOW() - INTERVAL 2 WEEK AND (coin_code LIKE '%{search_str}%' OR coin_name_kr LIKE '%{search_str}%' OR coin_name_en LIKE '%{search_str}%')
	"""

	region = Util.TryGetDictStr(req_dict, "stock_region", "")
	type = Util.TryGetDictStr(req_dict, "stock_type", "")
	offset = Util.TryGetDictInt(req_dict, "list_offset", 0)

	if type == "STOCK":
		stock_sql_query_str += " AND stock_type='STOCK'"
	elif type == "ETF":
		stock_sql_query_str += " AND stock_type='ETF'"
	elif type == "ETN":
		stock_sql_query_str += " AND stock_type='ETN'"

	if region == "KR":
		query_str = stock_sql_query_str + " AND (stock_market='KOSPI' OR stock_market='KOSDAQ' OR stock_market='KONEX')"
	elif region == "US":
		query_str = stock_sql_query_str + " AND (stock_market='NYSE' OR stock_market='NASDAQ' OR stock_market='AMEX')"
	elif region == "COIN":
		query_str = coin_sql_query_str
	else:
		query_str = stock_sql_query_str + " UNION " + coin_sql_query_str

	query_str += f" ORDER BY stock_code LIMIT 100 OFFSET {offset * 100}"

	sql_code, sql_data = await SqlManager.sql_manager.Get(query_str)
	if sql_code == 0:
		return 200, "success", { "list" : sql_data }
	else:
		return 500, "fail to get data", { "list" : sql_data }
	

async def GetRegistedQueryList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 3:
		return 400, "invalid permission", {}
	

	stock_sql_query_str = """
		SELECT 'STOCK' AS table_type, Q.stock_code, Q.query_type, I.stock_name_kr, I.stock_market
		FROM KoreaInvest.stock_last_ws_query AS Q
		JOIN KoreaInvest.stock_info AS I
		ON Q.stock_code = I.stock_code
	"""
	coin_sql_query_str = """
		SELECT 'COIN' AS table_type, Q.coin_code, Q.query_type, I.coin_name_kr, 'COIN' AS stock_market
		FROM Bithumb.coin_last_ws_query AS Q
		JOIN Bithumb.coin_info AS I
		ON Q.coin_code = I.coin_code
	"""
	
	region = ""
	if "region" in req_dict:
		region = req_dict["region"]
		
	if region == "KR":
		query_str = stock_sql_query_str + " WHERE I.stock_market='KOSPI' OR I.stock_market='KOSDAQ' OR I.stock_market='KONEX'"
	elif region == "US":
		query_str = stock_sql_query_str + " WHERE I.stock_market='NYSE' OR I.stock_market='NASDAQ' OR I.stock_market='AMEX'"
	elif region == "COIN":
		query_str = coin_sql_query_str
	else:
		query_str = stock_sql_query_str + " UNION " + coin_sql_query_str

	sql_code, sql_data = await SqlManager.sql_manager.Get(query_str)
	if sql_code == 0:
		return 200, "success", { "list" : sql_data }
	else:
		return 500, "fail to get data", { "list" : sql_data }


async def UpdateRegistedQueryList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 1:
		return 400, "invalid permission", {}
	

	target_info_list = req_dict["list"]	# table_type, target_code, query_type

	stock_insert_list = []
	code_type_dict = {target_info[1]: target_info[2] for target_info in target_info_list if target_info[0].upper() != "COIN"}
	if len(code_type_dict) > 0:
		code_list = list(code_type_dict.keys())
		code_strings = ','.join([f"'{code}'" for code in code_list])
		query_str = f"""
			SELECT stock_code, stock_market
			FROM KoreaInvest.stock_info
			WHERE stock_code IN ({code_strings}) AND stock_update > NOW() - INTERVAL 2 WEEK
		"""
		sql_code, sql_data = await SqlManager.sql_manager.Get(query_str)
		if sql_code != 0:
			return 500, "fail to get data", {}

		for query_ret in sql_data:
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
					f"{api_prefix}{code}",
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
	code_type_dict = {target_info[1]: target_info[2] for target_info in target_info_list if target_info[0].upper() == "COIN"}
	if len(code_type_dict) > 0:
		code_list = list(code_type_dict.keys())
		code_strings = ','.join([f"'{code}'" for code in code_list])
		query_str = f"""
			SELECT coin_code
			FROM Bithumb.coin_info
			WHERE coin_code IN ({code_strings}) AND coin_update > NOW() - INTERVAL 2 WEEK
		"""
		sql_code, sql_data = await SqlManager.sql_manager.Get(query_str)
		if sql_code != 0:
			return 500, "fail to get data", {}

		for query_ret in sql_data:
			code = query_ret[0]
			query_type = code_type_dict[code]

			if query_type == "EX":			# 지금은 체결내용만 받기로함
				coin_insert_list.append((
					f"{query_type}_{code}",
					code,
					query_type,
					"transaction",
					f"{code}_KRW",
				))

			elif query_type == "OD":
				# 아직 준비 안되어 있어서 주석처리
				return 500, "OD type is not available", {}
							
				coin_insert_list.append((
					f"{query_type}_{code}",
					code,
					query_type,
					"orderbooksnapshot",
					f"{code}_KRW",
				))
				
			else:
				return 400, "invalid query_type detected", {}


	query_str_list = [
		"DELETE FROM KoreaInvest.stock_last_ws_query",
		"DELETE FROM Bithumb.coin_last_ws_query",
	]
	if len(stock_insert_list) > 0:
		values_str = ', '.join([f"({", ".join([f"'{str(item)}'" for item in record])})" for record in stock_insert_list])
		query_str_list.append(f"""INSERT INTO KoreaInvest.stock_last_ws_query VALUES {values_str}""")
	if len(coin_insert_list) > 0:
		values_str = ', '.join([f"({", ".join([f"'{str(item)}'" for item in record])})" for record in coin_insert_list])
		query_str_list.append(f"""INSERT INTO Bithumb.coin_last_ws_query VALUES {values_str}""")

	sql_code = await SqlManager.sql_manager.Set(query_str_list)
	if sql_code == 0:
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

	sql_code, sql_data = await SqlManager.sql_manager.Get(query_str)
	if sql_code == 0:
		return 200, "success", { "candle" : sql_data }
	else:
		return 500, "fail to get data", { "candle" : sql_data }

	

