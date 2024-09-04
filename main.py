import websockets
import asyncio
import json
from datetime import datetime as DateTime

import module.Util as Util
import module.Auth as Auth
import module.ServiceWOL as WOL


send_lock = asyncio.Lock()

async def safe_send(ws, message):
	async with send_lock:
		await ws.send(message)


async def handler_none(ws:websockets.WebSocketServerProtocol):
	rep_data: dict = { "type": "rep", "result": 400, "msg": "Invalid service" }
	await safe_send(ws, json.dumps(rep_data))


async def handler_ping(ws:websockets.WebSocketServerProtocol):
	rep_data: dict = { "type": "ping", "result": 200, "msg": "good" }
	await safe_send(ws, json.dumps(rep_data))


async def handler_auth(ws:websockets.WebSocketServerProtocol, req_data:json) -> dict:
	rep_data: dict = { "type": "rep", "result": 500, "msg": "server_error" }
	client_info = {
		"ws_object": id(ws),
		"ip": ws.remote_address[0],
		"is_good_man": False,
		"ping": DateTime.now(),
		"user_info": {},
	}
	try:
		if req_data["type"] == "login":
			is_good, msg, info = await Auth.IsExistUser(req_data["data"]["id"], req_data["data"]["pw"])
			client_info["is_good_man"] = is_good
			client_info["user_info"] = info
			rep_data["result"] = 200 if is_good == True else 400
			rep_data["msg"] = msg
   
		else:
			rep_data["result"] = 400
			rep_data["msg"] = "Fail to authorization"

	finally:
		await safe_send(ws, json.dumps(rep_data))
		return client_info
	

async def handler_test(ws:websockets.WebSocketServerProtocol, req_data:json):
	rep_data: dict = { "type": "rep", "result": 500, "msg": "server_error" }
	try:
		if req_data["type"] == "type_list":
			rep_data["result"] = 200
			rep_data["msg"] = "good"
			rep_data["data"] = [
				"TEST1",
				"wol_device",
			]
			
		elif req_data["type"] == "TEST1":
			rep_data["result"] = 200
			rep_data["msg"] = "good"
			rep_data["data"] = [
				"TEST2",
			]

		elif req_data["type"] == "TEST2":
			rep_data["result"] = 200
			rep_data["msg"] = "good"
			rep_data["data"] = [
				"TEST2",
			]

		else:
			rep_data["result"] = 400
			rep_data["msg"] = "invalid type"

	finally:
		await safe_send(ws, json.dumps(rep_data))


async def handler_wol(ws:websockets.WebSocketServerProtocol, req_data:json):
	rep_data: dict = { "type": "rep", "result": 500, "msg": "server_error" }
	try:
		if req_data["type"] == "type_list":
			rep_data["result"] = 200
			rep_data["msg"] = "good"
			rep_data["data"] = [
				"wol_list",
				"wol_device",
			]
			
		elif req_data["type"] == "wol_list":
			rep_data["result"] = 200
			rep_data["msg"] = "good"
			rep_data["data"] = WOL.GetWOLList()

		elif req_data["type"] == "wol_device":
			if WOL.ExecuteWOL(req_data["data"]["device_name"]):
				rep_data["result"] = 200
				rep_data["msg"] = "good"
			else:
				rep_data["result"] = 500
				rep_data["msg"] = "server_error"

		else:
			rep_data["result"] = 400
			rep_data["msg"] = "invalid type"

	finally:
		await safe_send(ws, json.dumps(rep_data))


async def handler_main(ws:websockets.WebSocketServerProtocol, path:str):
	# 클라이언트의 IP 주소 얻기
	client_id = id(ws)
	client_ip = ws.remote_address[0]
	Util.InsertLog("WebSocketServer", "N", f"Client connected [ {client_id} | {client_ip} ]")
	
	disconnect_log_msg = "normal"
	try:
		auth_msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
		auth_data = json.loads(auth_msg)
		if path == "/bae" and auth_data["service"] == "auth":
			client_info = await handler_auth(ws, auth_data)
		else:
			client_info = await handler_none(ws)
		
		if client_info["is_good_man"] == False:
			raise Exception("Invalid authorization")

		while True:
			try:
				req_msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
				req_data = json.loads(req_msg)
				
				if req_data["service"] == "test":
					await handler_test(ws, req_data)
				
				elif req_data["service"] == "wol":
					await handler_wol(ws, req_data)
     
				elif req_data["service"] == "ping":
					await handler_ping(ws)
					
				else:
					await handler_none(ws)
					
				client_info["ping"] = DateTime.now()

			except asyncio.TimeoutError:
				delta_sec = (DateTime.now() - client_info["ping"]).seconds
				if delta_sec > 10:
					raise Exception("Detected late ping")
			except:
				await handler_none(ws)
				
	except asyncio.TimeoutError as ex:
		disconnect_log_msg = ex.__str__()
	except Exception as ex:
		disconnect_log_msg = ex.__str__()
	finally:
		Util.InsertLog(
			"WebSocketServer",
			"N",
			f"Client disconnected [ {client_id} | {client_ip} | {disconnect_log_msg} ]",
		)


if __name__ == "__main__":
	start_server = websockets.serve(handler_main, "0.0.0.0", 49693)
	Util.InsertLog("WebSocketServer", "N", f"Server start at port 49693")
	asyncio.get_event_loop().run_until_complete(start_server)
	asyncio.get_event_loop().run_forever()
