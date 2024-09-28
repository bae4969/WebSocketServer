import module.Util as Util
import module.SqlManager as SqlManager
import pymysql
from datetime import datetime as DateTime



async def LoginUser(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	if "id" not in req_dict or "pw" not in req_dict:
		return 500, "id or pw is not exist", {}

	id_str = req_dict["id"].replace("'", "")
	pw_str = req_dict["pw"].replace("'", "")
	query_str = f"SELECT user_index, user_level, user_state FROM Blog.user_list WHERE user_id='{id_str}' and user_pw='{pw_str}'"
	sql_code, sql_data = await SqlManager.sql_manager.Get(query_str)
	
	if len(sql_data) == 0:
		return 500, "not exist", {}
	
	client_info["user_id"] = id_str
	client_info["user_index"] = sql_data[0][0]
	client_info["user_level"] = sql_data[0][1]
	client_info["user_state"] = sql_data[0][2]
	client_info["is_good_man"] = client_info["user_level"] < 4 and client_info["user_state"] == 0
	if client_info["is_good_man"] == False:
		return 500, "invalid user state", {}
	
	query_str_list = [f"""
		INSERT INTO Blog.user_login (user_index, login_index, login_datetime, login_env, login_ip)
		VALUES ({client_info["user_index"]}, {client_info["login_index"]}, NOW(), '{client_info["login_env"]}', '{client_info["login_ip"]}')"""]
	sql_code = await SqlManager.sql_manager.Set(query_str_list)

	if sql_code == 0:
		query_str_list = [f"""UPDATE Blog.user_list SET user_last_action_datetime=NOW() WHERE user_index='{client_info["user_index"]}'"""]
		await SqlManager.sql_manager.Set(query_str_list)
		return 200, "success", {}
	
	# 중복키 insert시 중복 로그인이라 판단함
	elif sql_code == 1062:
		return 400, "already logined", {}
	
	else:
		return 400, "fail to insert", {}


async def LogoutUser(client_info:dict) -> tuple[int, str, dict]:
	query_str_list = [f"""DELETE FROM Blog.user_login WHERE user_index='{client_info["user_index"]}'"""]
	sql_code = await SqlManager.sql_manager.Set(query_str_list)
	if sql_code == 0:
		return 200, "success", {}
	else:
		return 400, "fail to delete", {}


async def PingUser(client_info:dict) -> tuple[int, str, dict]:
	client_info["ping"] = DateTime.now()
	return 200, "success", {}


async def VarifiyUser(client_info:dict) -> tuple[int, str, dict]:
	try:
		query_str_list = f"""SELECT COUNT(*) FROM Blog.user_login WHERE user_index={client_info["user_index"]} AND login_index={client_info["login_index"]}"""
		sql_code, sql_data = await SqlManager.sql_manager.Get(query_str_list)
		client_info["is_good_man"] = sql_data[0][0] > 0
		return 200, "success", {}
	except:
		client_info["is_good_man"] = False
		return 400, "fail to varifiy", {}
	

