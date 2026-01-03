import core.Util as Util
import core.SqlManager as SqlManager
from datetime import datetime
import re


PAGE_SIZE = 100


def _build_total_list_query(stock_region: str, stock_type: str, list_offset: int) -> str:
	stock_sql_query_str = f"""
		SELECT 'STOCK' AS table_type, stock_code, stock_name_kr, stock_market, stock_type
		FROM KoreaInvest.stock_info
		WHERE stock_update > NOW() - INTERVAL 2 WEEK
	"""
	coin_sql_query_str = f"""
		SELECT 'COIN' AS table_type, coin_code, coin_name_kr, 'COIN' AS stock_market, 'COIN' AS stock_type
		FROM Bithumb.coin_info
		WHERE coin_update > NOW() - INTERVAL 2 WEEK
	"""

	if stock_type == "STOCK":
		stock_sql_query_str += " AND stock_type='STOCK'"
	elif stock_type == "ETF":
		stock_sql_query_str += " AND stock_type='ETF'"
	elif stock_type == "ETN":
		stock_sql_query_str += " AND stock_type='ETN'"

	if stock_region == "KR":
		query_str = stock_sql_query_str + " AND (stock_market='KOSPI' OR stock_market='KOSDAQ' OR stock_market='KONEX')"
	elif stock_region == "US":
		query_str = stock_sql_query_str + " AND (stock_market='NYSE' OR stock_market='NASDAQ' OR stock_market='AMEX')"
	elif stock_region == "COIN":
		query_str = coin_sql_query_str
	else:
		query_str = stock_sql_query_str + " UNION " + coin_sql_query_str

	if stock_region == "COIN":
		query_str += f" ORDER BY coin_code LIMIT {PAGE_SIZE} OFFSET {list_offset * PAGE_SIZE}"
	else:
		query_str += f" ORDER BY stock_code LIMIT {PAGE_SIZE} OFFSET {list_offset * PAGE_SIZE}"

	return query_str


def _build_search_total_list_query(search_str: str, stock_region: str, stock_type: str, list_offset: int) -> str:
	search_words = list(filter(None, search_str.split()))
	stock_search_condition = ' AND '.join([f"stock_name_kr LIKE '%{word}%'" for word in search_words])
	coin_search_condition = ' AND '.join([f"coin_name_kr LIKE '%{word}%'" for word in search_words])

	stock_sql_query_str = f"""
		SELECT 'STOCK' AS table_type, stock_code, stock_name_kr, stock_market, stock_type
		FROM KoreaInvest.stock_info
		WHERE stock_update > NOW() - INTERVAL 2 WEEK AND (stock_code LIKE '{search_str}%' OR ({stock_search_condition}))
	"""
	coin_sql_query_str = f"""
		SELECT 'COIN' AS table_type, coin_code, coin_name_kr, 'COIN' AS stock_market, 'COIN' AS stock_type
		FROM Bithumb.coin_info
		WHERE coin_update > NOW() - INTERVAL 2 WEEK AND (coin_code LIKE '{search_str}%' OR ({coin_search_condition}))
	"""

	if stock_type == "STOCK":
		stock_sql_query_str += " AND stock_type='STOCK'"
	elif stock_type == "ETF":
		stock_sql_query_str += " AND stock_type='ETF'"
	elif stock_type == "ETN":
		stock_sql_query_str += " AND stock_type='ETN'"

	if stock_region == "KR":
		query_str = stock_sql_query_str + " AND (stock_market='KOSPI' OR stock_market='KOSDAQ' OR stock_market='KONEX')"
	elif stock_region == "US":
		query_str = stock_sql_query_str + " AND (stock_market='NYSE' OR stock_market='NASDAQ' OR stock_market='AMEX')"
	elif stock_region == "COIN":
		query_str = coin_sql_query_str
	else:
		query_str = stock_sql_query_str + " UNION " + coin_sql_query_str

	if stock_region == "COIN":
		query_str += f" ORDER BY coin_code LIMIT {PAGE_SIZE} OFFSET {list_offset * PAGE_SIZE}"
	else:
		query_str += f" ORDER BY stock_code LIMIT {PAGE_SIZE} OFFSET {list_offset * PAGE_SIZE}"

	return query_str


async def GetTotalList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 3:
		return 400, "invalid permission", {}
	
	stock_region = Util.TryGetDictStr(req_dict, "stock_region", "")
	stock_type = Util.TryGetDictStr(req_dict, "stock_type", "")
	list_offset = max(0, Util.TryGetDictInt(req_dict, "list_offset", 0))
	query_str = _build_total_list_query(stock_region, stock_type, list_offset)

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

	stock_region = Util.TryGetDictStr(req_dict, "stock_region", "")
	stock_type = Util.TryGetDictStr(req_dict, "stock_type", "")
	list_offset = max(0, Util.TryGetDictInt(req_dict, "list_offset", 0))

	search_words = list(filter(None, search_str.split()))
	stock_search_condition = ' AND '.join(["stock_name_kr LIKE %s" for _ in search_words])
	coin_search_condition = ' AND '.join(["coin_name_kr LIKE %s" for _ in search_words])

	stock_sql_query_str = (
		"""
		SELECT 'STOCK' AS table_type, stock_code, stock_name_kr, stock_market, stock_type
		FROM KoreaInvest.stock_info
		WHERE stock_update > NOW() - INTERVAL 2 WEEK AND (stock_code LIKE %s OR ("""
		+ stock_search_condition
		+ "))\n"
	)
	coin_sql_query_str = (
		"""
		SELECT 'COIN' AS table_type, coin_code, coin_name_kr, 'COIN' AS stock_market, 'COIN' AS stock_type
		FROM Bithumb.coin_info
		WHERE coin_update > NOW() - INTERVAL 2 WEEK AND (coin_code LIKE %s OR ("""
		+ coin_search_condition
		+ "))\n"
	)

	stock_params = [f"{search_str}%"] + [f"%{word}%" for word in search_words]
	coin_params = [f"{search_str}%"] + [f"%{word}%" for word in search_words]

	if stock_type == "STOCK":
		stock_sql_query_str += " AND stock_type='STOCK'"
	elif stock_type == "ETF":
		stock_sql_query_str += " AND stock_type='ETF'"
	elif stock_type == "ETN":
		stock_sql_query_str += " AND stock_type='ETN'"

	params = None
	if stock_region == "KR":
		query_str = stock_sql_query_str + " AND (stock_market='KOSPI' OR stock_market='KOSDAQ' OR stock_market='KONEX')"
		query_str += f" ORDER BY stock_code LIMIT {PAGE_SIZE} OFFSET {list_offset * PAGE_SIZE}"
		params = tuple(stock_params)
	elif stock_region == "US":
		query_str = stock_sql_query_str + " AND (stock_market='NYSE' OR stock_market='NASDAQ' OR stock_market='AMEX')"
		query_str += f" ORDER BY stock_code LIMIT {PAGE_SIZE} OFFSET {list_offset * PAGE_SIZE}"
		params = tuple(stock_params)
	elif stock_region == "COIN":
		query_str = coin_sql_query_str
		query_str += f" ORDER BY coin_code LIMIT {PAGE_SIZE} OFFSET {list_offset * PAGE_SIZE}"
		params = tuple(coin_params)
	else:
		query_str = stock_sql_query_str + " UNION " + coin_sql_query_str
		query_str += f" ORDER BY stock_code LIMIT {PAGE_SIZE} OFFSET {list_offset * PAGE_SIZE}"
		params = tuple(stock_params + coin_params)

	sql_code, sql_data = await SqlManager.sql_manager.Get(query_str, params)
	if sql_code == 0:
		return 200, "success", { "list" : sql_data }
	else:
		return 500, "fail to get data", { "list" : sql_data }
	

async def GetRegistedQueryList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 3:
		return 400, "invalid permission", {}
	

	stock_sql_query_str = """
		SELECT 'STOCK' AS table_type, Q.stock_code, Q.query_type, I.stock_name_kr, I.stock_market, I.stock_type
		FROM KoreaInvest.stock_last_ws_query AS Q
		JOIN KoreaInvest.stock_info AS I
		ON Q.stock_code = I.stock_code
	"""
	coin_sql_query_str = """
		SELECT 'COIN' AS table_type, Q.coin_code, Q.query_type, I.coin_name_kr, 'COIN' AS stock_market, 'COIN' AS stock_type
		FROM Bithumb.coin_last_ws_query AS Q
		JOIN Bithumb.coin_info AS I
		ON Q.coin_code = I.coin_code
	"""
	
	region = Util.TryGetDictStr(req_dict, "region", "")
		
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
		placeholders = ','.join(['%s' for _ in code_list])
		query_str = f"""
			SELECT stock_code, stock_market
			FROM KoreaInvest.stock_info
			WHERE stock_code IN ({placeholders}) AND stock_update > NOW() - INTERVAL 2 WEEK
		"""
		sql_code, sql_data = await SqlManager.sql_manager.Get(query_str, tuple(code_list))
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
		placeholders = ','.join(['%s' for _ in code_list])
		query_str = f"""
			SELECT coin_code
			FROM Bithumb.coin_info
			WHERE coin_code IN ({placeholders}) AND coin_update > NOW() - INTERVAL 2 WEEK
		"""
		sql_code, sql_data = await SqlManager.sql_manager.Get(query_str, tuple(code_list))
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
		row_placeholders = ','.join(['(%s,%s,%s,%s,%s)' for _ in stock_insert_list])
		flat_params = []
		for record in stock_insert_list:
			flat_params.extend(list(record))
		query_str_list.append((
			f"INSERT INTO KoreaInvest.stock_last_ws_query VALUES {row_placeholders}",
			tuple(flat_params),
		))
	if len(coin_insert_list) > 0:
		row_placeholders = ','.join(['(%s,%s,%s,%s,%s)' for _ in coin_insert_list])
		flat_params = []
		for record in coin_insert_list:
			flat_params.extend(list(record))
		query_str_list.append((
			f"INSERT INTO Bithumb.coin_last_ws_query VALUES {row_placeholders}",
			tuple(flat_params),
		))

	sql_code = await SqlManager.sql_manager.Set(query_str_list)
	if sql_code == 0:
		return 200, "success", {}
	else:
		return 500, "fail to update", {}


async def GetCandleData(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if client_info["user_level"] > 3:
		return 400, "invalid permission", {}
	
	table_type_raw = Util.TryGetDictStr(req_dict, "table_type", "").upper()
	if table_type_raw not in ("STOCK", "COIN"):
		return 400, "invalid table_type", {}
	table_type = "Stock" if table_type_raw == "STOCK" else "Coin"

	target_code = Util.TryGetDictStr(req_dict, "target_code", "").replace("/", "_")
	if not re.fullmatch(r"[A-Za-z0-9_]{1,32}", target_code):
		return 400, "invalid target_code", {}
	
	# Python isocalendar는 월요일 시작이므로 MySQL Mode 0(일요일 시작)과 맞추기 위해 기본값 처리 유의
	now = datetime.now()
	year = Util.TryGetDictInt(req_dict, "year", now.year)
	week_from = Util.TryGetDictInt(req_dict, "week_from", now.isocalendar()[1])
	week_to = Util.TryGetDictInt(req_dict, "week_to", now.isocalendar()[1])
	if year < 2000 or year > 2100:
		return 400, "invalid year", {}
	week_from = max(1, min(53, week_from))
	week_to = max(1, min(53, week_to))

	# SQL 수정: YEARWEEK 대신 WEEK 사용 (1월 초 데이터 누락 방지)
	# WEEK(date, 0) + 1을 하면 1월 1일~첫 일요일 구간이 1주차가 됩니다.
	schema_name = f"Z_{table_type}{target_code}"
	table_name = f"Candle{year:04d}"
	query_str = f"""
		SELECT 
			DATE_FORMAT(execution_datetime, '%%Y%%m%%d%%H%%i%%s') AS execution_datetime, 
			execution_open, execution_close, execution_min, execution_max, 
			execution_non_volume, execution_ask_volume, execution_bid_volume
		FROM `{schema_name}`.`{table_name}`
		WHERE WEEK(execution_datetime, 0) + 1 BETWEEN %s AND %s;
	"""

	sql_code, sql_data = await SqlManager.sql_manager.Get(query_str, (week_from, week_to))

	if sql_code == 0:
		return 200, "success", { "candle" : sql_data }
	else:
		return 500, "fail to get data", { "candle" : sql_data }
	

