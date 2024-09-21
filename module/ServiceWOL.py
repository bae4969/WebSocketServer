import module.Util as Util
import module.SqlManager as SqlManager
from doc import Define
import struct
import socket



async def ExecuteWOL(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	dev_name = req_dict["device_name"]

	ip_and_mac:list = Define.WOL_DEVICE_LIST[dev_name]
	addrs = ip_and_mac[1].split("-")
	hw_addr = struct.pack(
		"BBBBBB",
		int(addrs[0], 16),
		int(addrs[1], 16),
		int(addrs[2], 16),
		int(addrs[3], 16),
		int(addrs[4], 16),
		int(addrs[5], 16),
	)

	magic = b"\xFF" * 6 + hw_addr * 16

	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
	s.sendto(magic, (ip_and_mac[0], 9))
	s.close()

	return 0, "success", {}


async def GetWOLList(client_info:dict, req_dict:dict) -> tuple[int, str, dict]:
	return 0, "success", { "list" : [*Define.WOL_DEVICE_LIST] }
