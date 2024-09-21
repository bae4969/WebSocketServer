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
						user_ping_datetime DATETIME NULL DEFAULT CURRENT_TIMESTAMP(),
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
	
	



