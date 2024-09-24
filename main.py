import websockets
import asyncio
import json
from datetime import datetime as DateTime

import doc.Define as Define
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
        return json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
    except asyncio.TimeoutError:
        return {
			"service" : "late",
			"work" : "err",
			"data" : {}
		}



async def handler_invalid_service(ws:websockets.WebSocketServerProtocol, req_service:str) -> None:
	rep_data: dict = { "service": req_service, "result": 400, "msg": "invalid service", "data":{} }
	await safe_send(ws, json.dumps(rep_data))

async def handler_auth(ws:websockets.WebSocketServerProtocol, client_info:dict, req_work:str, req_dict:dict) -> None:
	rep_data: dict = { "service": "auth", "result": 500, "msg": "server_error", "data":{} }
	try:
		if req_work == "login":
			result, msg, rep_dict = await Auth.LoginUser(client_info, req_dict)

		elif req_work == "logout":
			result, msg, rep_dict = await Auth.LogoutUser(client_info)
		
		elif req_work == "ping":
			result, msg, rep_dict = await Auth.LogoutUser(client_info)
			
		else:
			result = 400
			msg = "invalid service type"
			rep_dict = {}


		rep_data["result"] = result
		rep_data["msg"] = msg
		rep_data["data"] = rep_dict

	finally:
		if client_info["is_good_man"] == False:
			await asyncio.sleep(1)
		await safe_send(ws, json.dumps(rep_data))

async def handler_wol(ws:websockets.WebSocketServerProtocol, client_info:dict, req_work:str, req_dict:dict) -> None:
	rep_data: dict = { "service": "wol", "result": 500, "msg": "server_error", "data":{} }
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
	rep_data: dict = { "service": "wtm", "result": 500, "msg": "server_error", "data":{} }
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
	log_postfix = f"{client_info['ws_id']} | {client_info['ws_ip']}"
	Util.InsertLog("WebSocketServer", "N", f"Client connected [ {log_postfix} ]")
	
	disconnect_log_msg = "normal"
	try:
		req_data = await safe_recv(ws)
		await handler_auth(ws, client_info, "login", req_data["data"])
		if client_info["is_good_man"] == False:
			raise Exception("invalid authorization")

		Util.InsertLog("WebSocketServer", "N", f"User '{client_info["user_id"]}({client_info["user_index"]})' login [ {log_postfix} ]")
		while ws.closed == False:
			try:
				req_data = await safe_recv(ws)
				req_service:str = req_data["service"]
				req_work:str = req_data["work"]
				req_dict:dict = req_data["data"]
				
				if req_service == "late":
					pass
			
				elif req_service == "auth":
					await handler_auth(ws, client_info, req_work, req_dict)

				elif req_service == "wol":
					await handler_wol(ws, client_info, req_work, req_dict)

				elif req_service == "stm":
					await handler_stm(ws, client_info, req_work, req_dict)
					
				else:
					await handler_invalid_service(ws, req_service)

			except:
				pass
						
			delta_sec = (DateTime.now() - client_info["ping"]).seconds
			if delta_sec > Define.WS_LATE_PING_SEC:
				raise Exception("Detected late ping")

				
	except Exception as ex:
		disconnect_log_msg = str(ex)
	finally:
		try:
			if client_info["user_index"] >= 0:
				await handler_auth(ws, "auth", "logout", client_info)
				Util.InsertLog("WebSocketServer", "N", f"User '{client_info["user_id"]}({client_info["user_index"]})' logout [ {log_postfix} ]")
		except:
			pass
		Util.InsertLog(
			"WebSocketServer",
			"N",
			f"Client disconnected [ {log_postfix} | {disconnect_log_msg} ]",
		)


if __name__ == "__main__":
	if asyncio.get_event_loop().run_until_complete(SqlManager.InitSql()) == False:
		Util.InsertLog("WebSocketServer", "N", f"Fail to init login table")
	
	else:
		start_server = websockets.serve(handler_main, "0.0.0.0", 49693)
		Util.InsertLog("WebSocketServer", "N", f"Server start at port 49693")
		asyncio.get_event_loop().run_until_complete(start_server)
		asyncio.get_event_loop().run_forever()
