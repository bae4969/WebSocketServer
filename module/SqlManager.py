import doc.Define as Define
import aiomysql
import pymysql


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
				login_index BIGINT UNSIGNED NOT NULL,
				login_datetime DATETIME NULL DEFAULT CURRENT_TIMESTAMP(),
				login_env VARCHAR(64) NULL,
				login_ip VARCHAR(16) NULL,
				PRIMARY KEY (user_index),
				CONSTRAINT user_index FOREIGN KEY (user_index) REFERENCES Blog.user_list (user_index) ON UPDATE CASCADE ON DELETE CASCADE
			)
			COLLATE='utf8mb4_general_ci'
			ENGINE=MEMORY"""

			sql_code = await self.Set([ query_str1, query_str2 ])
			return sql_code == 0
   
		except:
			return False
		

	async def Set(self, query_str_list:list[str]) -> int:
		try:
			code = -1
			async with self.__sql_pool.acquire() as conn:
				async with conn.cursor() as cur:
					try:
						await conn.begin()
						for query_str in query_str_list:
							await cur.execute(query_str)
						await conn.commit()
						code = 0

					except pymysql.err.IntegrityError as ex:
						await conn.rollback()
						code = ex.args[0]

					except:
						await conn.rollback()
						code = -1


			return code
		
		except:
			return -1
		

	async def Get(self, query_str:str) -> tuple[int, list]:
		try:
			code = -1
			data = []
			async with self.__sql_pool.acquire() as conn:
				async with conn.cursor() as cur:
					try:
						await cur.execute(query_str)
						data = await cur.fetchall()
						code = 0

					except pymysql.err.IntegrityError as ex:
						await conn.rollback()
						code = ex.args[0]

					except:
						await conn.rollback()
						code = -1

			return code, data

		except:
			return -1, []
		


sql_manager = SqlManager()

async def InitSql() -> bool:
	return await sql_manager.Init()
	
	



