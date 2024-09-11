import doc.Define as Define
import aiomysql


class SqlManager:
	__sql_pool = None
	
	async def Init(self) -> bool:
		try:
			self.__sql_pool = await aiomysql.create_pool(
				host= Define.SQL_HOST,
				port=3306,
				user=Define.SQL_ID,
				password=Define.SQL_PW,
				db='Blog',
				minsize=2,
				maxsize=10
			)
   
			async with self.__sql_pool.acquire() as conn:
				async with conn.cursor() as cur:
					query_str1 = f"DROP TABLE IF EXISTS user_login"
					query_str2 = f"""CREATE TABLE user_login (
						user_index INT UNSIGNED NOT NULL,
						user_login_datetime DATETIME NULL DEFAULT CURRENT_TIMESTAMP(),
						user_login_env VARCHAR(64) NULL,
						user_login_ip VARCHAR(16) NULL,
						PRIMARY KEY (user_index),
						CONSTRAINT user_index FOREIGN KEY (user_index) REFERENCES user_list (user_index) ON UPDATE CASCADE ON DELETE CASCADE
					)
					COLLATE='utf8mb4_general_ci'
					ENGINE=MEMORY"""

					await cur.execute(query_str1)
					await cur.execute(query_str2)

			return True
		except:
			return False
	async def Set(self, query_str:str) -> bool:
		try:
			async with self.__sql_pool.acquire() as conn:
				async with conn.cursor() as cur:
					await cur.execute(query_str)

			return True
		except:
			return False
	async def Get(self, query_str:str) -> list:
		try:
			async with self.__sql_pool.acquire() as conn:
				async with conn.cursor() as cur:
					await cur.execute(query_str)
					return await cur.fetchall()

		except:
			return []
		

sql_manager = SqlManager()

async def InitSql() -> bool:
	return await sql_manager.Init()
			

async def IsExistUser(id:str, pw:str) -> tuple[bool, str, dict]:
	query_str = f"SELECT user_index, user_level, user_state from Blog.user_list WHERE user_id='{id}' and user_pw='{pw}'"
	last_query_list = await sql_manager.Get(query_str)
	
	if len(last_query_list) == 0:
		return False, "not exist", {}

	result = {
		"user_index": last_query_list[0][0],
		"user_level": last_query_list[0][1],
		"user_state": last_query_list[0][2],
	}
	
	if result["user_level"] > 1 or result["user_state"] > 0:
		return False, "invalid user", {}

	query_str = f"UPDATE user_list SET user_last_action_datetime=NOW() WHERE user_index='{result["user_index"]}'"
	await sql_manager.Set(query_str)
	
	return True, "success to authorization", result

async def LoginUser(client_info:dict):
	query_str = f"""INSERT INTO user_login (user_index, user_login_datetime, user_login_env, user_login_ip)
		VALUES ('{client_info["user_index"]}', NOW(), '--', '{client_info["ws_ip"]}')
		ON DUPLICATE KEY UPDATE
		user_login_datetime = NOW(),
		user_login_env = '--',
		user_login_ip = '{client_info["ws_ip"]}'"""
	await sql_manager.Set(query_str)
	
	



