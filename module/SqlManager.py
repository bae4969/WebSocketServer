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
				minsize=2,
				maxsize=10
			)

			query_str1 = f"DROP TABLE IF EXISTS Blog.user_login"
			query_str2 = f"""CREATE TABLE Blog.user_login (
				user_index INT UNSIGNED NOT NULL,
				user_login_datetime DATETIME NULL DEFAULT CURRENT_TIMESTAMP(),
				user_ping_datetime DATETIME NULL DEFAULT CURRENT_TIMESTAMP(),
				user_login_env VARCHAR(64) NULL,
				user_login_ip VARCHAR(16) NULL,
				PRIMARY KEY (user_index),
				CONSTRAINT user_index FOREIGN KEY (user_index) REFERENCES Blog.user_list (user_index) ON UPDATE CASCADE ON DELETE CASCADE
			)
			COLLATE='utf8mb4_general_ci'
			ENGINE=MEMORY"""

			return await self.Set([ query_str1, query_str2 ])
   
		except:
			return False
	async def Set(self, query_str_list:list[str]) -> bool:
		try:
			is_good = False
			async with self.__sql_pool.acquire() as conn:
				async with conn.cursor() as cur:
					try:
						await conn.begin()
						for query_str in query_str_list:
							await cur.execute(query_str)
						await conn.commit()
						is_good = True

					except:
						await conn.rollback()
						is_good = False

			return is_good
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
	
	



