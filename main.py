import websockets
import asyncio
import json
from datetime import datetime as DateTime

import module.Util as Util
import module.SqlManager as SqlManager
import module.Auth as Auth
import module.ServiceWOL as WOL
import module.StockTickerManager as STM



send_lock = asyncio.Lock()
async def safe_send(ws:websockets.WebSocketServerProtocol, message) -> None:
	async with send_lock:
		await ws.send(message)

async def safe_recv(ws:websockets.WebSocketServerProtocol):
    try:
        return json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0))
    except asyncio.TimeoutError:
        return {
			"type" : "err",
			"service" : "err",
			"work" : "err",
			"data" : {}
		}



async def handler_none(ws:websockets.WebSocketServerProtocol) -> None:
	rep_data: dict = { "type": "rep", "result": 400, "msg": "invalid msg" }
	await safe_send(ws, json.dumps(rep_data))

async def handler_ping(ws:websockets.WebSocketServerProtocol, client_info:dict) -> None:
	await Auth.PingUser(client_info)

async def handler_login(ws:websockets.WebSocketServerProtocol, client_info:dict, req_dict:dict) -> None:
	rep_data: dict = { "type": "rep", "result": 500, "msg": "server_error" }
	try:
		result, msg, rep_dict = await Auth.LoginUser(client_info, req_dict)		
		rep_data["result"] = result
		rep_data["msg"] = msg
		rep_data["data"] = rep_dict

	finally:
		# 지연처리
		if client_info["is_good_man"] == False:
			await asyncio.sleep(1)
		await safe_send(ws, json.dumps(rep_data))

async def handler_logout(ws:websockets.WebSocketServerProtocol, client_info:dict) -> None:
	rep_data: dict = { "type": "rep", "result": 500, "msg": "server_error" }
	try:
		result, msg, rep_dict = await Auth.LogoutUser(client_info)		
		rep_data["result"] = result
		rep_data["msg"] = msg
		rep_data["data"] = rep_dict

	finally:
		await safe_send(ws, json.dumps(rep_data))
	


async def handler_wol(ws:websockets.WebSocketServerProtocol, client_info:dict, req_work:str, req_dict:dict) -> None:
	rep_data: dict = { "type": "rep", "result": 500, "msg": "server_error" }
	try:
		if req_work == "list":
			result, msg, rep_dict = await WOL.GetWOLList(client_info, req_dict)

		elif req_work == "execute":
			result, msg, rep_dict = await WOL.ExecuteWOL(client_info, req_dict)

		else:
			result = 400
			msg = "invalid service type"
			rep_dict = {}

		
		rep_data["result"] = result
		rep_data["msg"] = msg
		rep_data["data"] = rep_dict

	finally:
		await safe_send(ws, json.dumps(rep_data))

async def handler_stm(ws:websockets.WebSocketServerProtocol, client_info:dict, req_work:str, req_dict:dict) -> None:
	rep_data: dict = { "type": "rep", "result": 500, "msg": "server_error" }
	try:
		if req_work == "get_tot_list":
			result, msg, rep_dict = await STM.GetTotalList(client_info, req_dict)

		elif req_work == "get_regi_list":
			result, msg, rep_dict = await STM.GetRegistedQueryList(client_info, req_dict)

		elif req_work == "update_regi_list":
			result, msg, rep_dict = await STM.UpdateRegistedQueryList(client_info, req_dict)

		else:
			result = 400
			msg = "invalid service type"
			rep_dict = {}

		
		rep_data["result"] = result
		rep_data["msg"] = msg
		rep_data["data"] = rep_dict

	finally:
		await safe_send(ws, json.dumps(rep_data))



async def handler_main(ws:websockets.WebSocketServerProtocol, path:str):
	# 클라이언트의 IP 주소 얻기
	client_info = {
		"ws_object": ws,
		"ws_id" : id(ws),
		"ws_ip": ws.remote_address[0],
		"is_good_man": False,
		"user_id" : "(empty)",
		"user_index" : -1,
		"user_level" : 100,
		"user_state" : 100,
		"ping": DateTime.now(),
	}
	Util.InsertLog("WebSocketServer", "N", f"Client connected [ {client_info['ws_id']} | {client_info['ws_ip']} ]")
	
	disconnect_log_msg = "normal"
	try:
		req_data = await safe_recv(ws)
		await handler_login(ws, client_info, req_data["data"])
		if client_info["is_good_man"] == False:
			raise Exception("invalid authorization")

		Util.InsertLog("WebSocketServer", "N", f"User '{client_info["user_id"]}({client_info["user_index"]})' login [ {client_info['ws_id']} | {client_info['ws_ip']} ]")
		while True:
			try:
				req_data = await safe_recv(ws)
				req_type:str = req_data["type"]
				req_service:str = req_data["service"]
				req_work:str = req_data["work"]
				req_dict:dict = req_data["data"]
				
				if req_service== "ping":
					await Auth.PingUser(client_info, req_dict)

				elif req_service == "wol":
					await handler_wol(ws, client_info, req_work, req_dict)

				elif req_service == "stm":
					await handler_stm(ws, client_info, req_work, req_dict)
					
				else:
					if req_type == "req":
						await handler_none(ws)

			except:
				pass
				
	except Exception as ex:
		disconnect_log_msg = ex.__str__()
	finally:
		try:
			if client_info["user_index"] >= 0:
				handler_logout()
		except:
			pass
		Util.InsertLog(
			"WebSocketServer",
			"N",
			f"Client disconnected [ {client_info['ws_id']} | {client_info['ws_ip']} | {disconnect_log_msg} ]",
		)


if __name__ == "__main__":
	if asyncio.get_event_loop().run_until_complete(SqlManager.InitSql()) == False:
		Util.InsertLog("WebSocketServer", "N", f"Fail to init login table")
	
	else:
		start_server = websockets.serve(handler_main, "0.0.0.0", 49693)
		Util.InsertLog("WebSocketServer", "N", f"Server start at port 49693")
		asyncio.get_event_loop().run_until_complete(start_server)
		asyncio.get_event_loop().run_forever()
