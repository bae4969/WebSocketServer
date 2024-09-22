import module.Util as Util
import module.SqlManager as SqlManager
from datetime import datetime as DateTime



async def LoginUser(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	id_str = req_dict["id"].replace("'", "")
	pw_str = req_dict["pw"].replace("'", "")
	query_str = f"SELECT user_index, user_level, user_state FROM Blog.user_list WHERE user_id='{id_str}' and user_pw='{pw_str}'"
	last_query_list = await SqlManager.sql_manager.Get(query_str)
	
	if len(last_query_list) == 0:
		return 500, "not exist", {}
	
	client_info["user_id"] = id_str
	client_info["user_index"] = last_query_list[0][0]
	client_info["user_level"] = last_query_list[0][1]
	client_info["user_state"] = last_query_list[0][2]
	client_info["is_good_man"] = client_info["user_level"] < 4 and client_info["user_state"] == 0
	if client_info["is_good_man"] == False:
		return 500, "invalid user state", {}
	
	query_str_list = [f"""
		INSERT INTO Blog.user_login (user_index, user_login_datetime, user_login_env, user_login_ip)
		VALUES ('{client_info["user_index"]}', NOW(), '--', '{client_info["ws_ip"]}')
		ON DUPLICATE KEY UPDATE
		user_login_datetime = NOW(),
		user_ping_datetime = NOW(),
		user_login_env = '--',
		user_login_ip = '{client_info["ws_ip"]}'
		"""]
	is_good_login = await SqlManager.sql_manager.Set(query_str_list)

	if is_good_login:
		query_str_list = [f"""UPDATE Blog.user_list SET user_last_action_datetime=NOW() WHERE user_index='{client_info["user_index"]}'"""]
		await SqlManager.sql_manager.Set(query_str_list)
		return 0, "success", {}
	
	else:
		return 400, "fail to insert", {}


async def LogoutUser(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	query_str_list = [f"""DELETE FROM Blog.user_login WHERE user_index='{client_info["user_index"]}'"""]
	result = await SqlManager.sql_manager.Set(query_str_list)
	if result:
		return 0, "success", {}
	else:
		return 400, "fail to delete", {}
	

async def PingUser(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	query_str_list = [f"""UPDATE Blog.user_login SET user_ping_datetime=NOW() WHERE user_index='{client_info["user_index"]}'"""]
	result = await SqlManager.sql_manager.Set(query_str_list)
	if result:
		client_info["ping"] = DateTime.now()
		return 0, "success", {}
	else:
		return 400, "fail to update", {}

