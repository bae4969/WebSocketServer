import doc.Define as Define
import pymysql

sql_query_connection = pymysql.connect(
	host = Define.SQL_HOST,
	port = 3306,
	user = Define.SQL_ID,
	passwd = Define.SQL_PW,
	db = "Blog",
	charset = 'utf8',
	autocommit=True,
)

def IsExistUser(id:str, pw:str) -> tuple[bool, str, dict]:
    query_str = f"Select user_level, user_state from Blog.user_list where user_id='{id}' and user_pw='{pw}'"
    
    sql_query_connection.ping(reconnect=True)
    cursor = sql_query_connection.cursor()
    cursor.execute(query_str)
    last_query_list = cursor.fetchall()
    
    if len(last_query_list) == 0:
        return False, "not exist", {}
    
    result = {
        "user_level": last_query_list[0][0],
        "user_state": last_query_list[0][1],
    }
    
    if result["user_level"] > 1 or result["user_state"] > 0:
        return False, "invalid user", {}
    
    return True, "success to authorization", result
    

    

