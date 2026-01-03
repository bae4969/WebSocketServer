import doc.Define as Define
import aiomysql
import pymysql


class SqlManager:
	__sql_pool = None

	@staticmethod
	def _normalize_query_item(item):
		# Supports both legacy list[str] and new list[tuple[str, tuple|list|dict]]
		if isinstance(item, tuple) and len(item) == 2:
			return item[0], item[1]
		return item, None
	
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
		

	async def Set(self, query_str_list:list) -> int:
		try:
			async with self.__sql_pool.acquire() as conn:
				async with conn.cursor() as cur:
					try:
						await conn.begin()
						for item in query_str_list:
							query_str, params = self._normalize_query_item(item)
							if params is None:
								await cur.execute(query_str)
							else:
								await cur.execute(query_str, params)
						await conn.commit()
						return 0
					except pymysql.err.IntegrityError as ex:
						await conn.rollback()
						return ex.args[0]
					except:
						await conn.rollback()
						return -1
		except:
			return -1
		

	async def Get(self, query_str:str, params=None) -> tuple[int, list]:
		try:
			async with self.__sql_pool.acquire() as conn:
				async with conn.cursor() as cur:
					try:
						if params is None:
							await cur.execute(query_str)
						else:
							await cur.execute(query_str, params)
						data = await cur.fetchall()
						return 0, data
					except pymysql.err.IntegrityError as ex:
						try:
							await conn.rollback()
						except:
							pass
						return ex.args[0], []
					except:
						try:
							await conn.rollback()
						except:
							pass
						return -1, []
		except:
			return -1, []
		


sql_manager = SqlManager()


async def InitSql() -> bool:
	return await sql_manager.Init()
