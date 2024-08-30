import websockets
import asyncio
import json
from datetime import datetime as DateTime

import module.Util as Util
import module.Auth as Auth
import module.ServiceWOL as WOL

clients = {}


async def handler_none(ws: websockets.WebSocketServerProtocol, type:str):
    rep_data: dict = {"result": 500}
    try:
        if type == "non_path":
            rep_data["result"] = 400
            rep_data["msg"] = "Invalid path"

        else:
            rep_data["result"] = 500
            rep_data["msg"] = "Error Server"

    finally:
        await ws.send(json.dumps(rep_data))
    

async def handler_auth(ws: websockets.WebSocketServerProtocol, req_data: dict):
    rep_data: dict = {"result": 500}
    try:
        if req_data["type"] == "login":
            is_good, msg, info = Auth.IsExistUser(req_data["id"], req_data["pw"])
            clients[id(ws)]["is_good_man"] = is_good
            clients[id(ws)]["user_info"] = info
            rep_data["result"] = 200 if is_good == True else 400
            rep_data["msg"] = msg

        elif req_data["type"] == "ping":
            clients[id(ws)]["ping"] = DateTime.now()

        else:
            rep_data["result"] = 400
            rep_data["msg"] = "Fail to authorization"

    finally:
        await ws.send(json.dumps(rep_data))


async def handler_test(ws: websockets.WebSocketServerProtocol, req_data: dict):
    rep_data: dict = {"result": 500}
    try:
        if req_data["type"] == "TEST1":
            clients[id(ws)]["is_good_man"] = True
            rep_data["result"] = 200
            rep_data["msg"] = "TEST1"

        elif req_data["type"] == "TEST2":
            clients[id(ws)]["is_good_man"] = True
            rep_data["result"] = 200
            rep_data["msg"] = "TEST2"

        else:
            rep_data["result"] = 400
            rep_data["type_list"] = {
                "TEST1",
                "TEST2",
            }

    finally:
        await ws.send(json.dumps(rep_data))


async def handler_wol(ws: websockets.WebSocketServerProtocol, req_data: dict):
    rep_data: dict = {"result": 500}
    try:
        if req_data["type"] == "wol_list":
            rep_data["result"] = 200
            rep_data["device_names"] = WOL.GetWOLList()

        elif req_data["type"] == "wol_device":
            if WOL.ExecuteWOL(req_data["device_name"]):
                rep_data["result"] = 200
            else:
                rep_data["result"] = 500

        else:
            rep_data["result"] = 400
            rep_data["type_list"] = {
                "wol_list",
                "wol_device",
            }

    finally:
        await ws.send(json.dumps(rep_data))


async def handler_main(ws: websockets.WebSocketServerProtocol, path: str):
    # 클라이언트의 IP 주소 얻기
    client_id = id(ws)
    client_ip = ws.remote_address[0]
    clients[client_id] = {
        "ws_object": ws,
        "ip": client_ip,
        "is_good_man": False,
        "user_info": {},
    }
    print(f"Client connected [ {client_id} | {client_ip} ]")

    disconnect_log_msg = "normal"
    try:
        auth_msg = ws.recv()
        auth_data = json.loads(auth_msg)
        if path == "/auth":
            await handler_auth(ws, auth_data)
        else:
            await handler_none(ws, "non_path")
        

        if clients[client_id]["is_good_man"] == False:
            raise Exception("Invalid authorization")

        while True:
            try:
                req_msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                req_data = json.loads(req_msg)
                
                if path == "/test":
                    await handler_test(ws, req_data)
                
                elif path == "/wol":
                    await handler_wol(ws, req_data)
                    
                else:
                    await handler_none(ws, "non_path")


            except asyncio.TimeoutError:
                delta_sec = (DateTime.now() - clients[client_id]["ping"]).seconds
                if delta_sec > 10:
                    raise Exception("Detected late ping")
            except:
                await handler_none(ws, "??")
                

    except Exception as ex:
        disconnect_log_msg = ex.__str__()
    finally:
        clients.pop(client_id)
        Util.InsertLog(
            f"Private WebSocket",
            "N",
            f"Client disconnected [ {client_id} | {client_ip} | {disconnect_log_msg} ]",
        )


if __name__ == "__main__":
    start_server = websockets.serve(handler_main, "0.0.0.0", 49693)
    Util.InsertLog(f"Private WebSocket", "N", f"server started on port 49693")
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
