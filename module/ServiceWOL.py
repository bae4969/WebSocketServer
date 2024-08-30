from doc import Define
import struct
import socket

targetLists:dict = {
	'Bae-DeskTop' : ['135.135.135.100', 'D8-5E-D3-E5-48-81'],
}

def ExecuteWOL(target_name:str) -> bool:
	try:
		ip_mac:list = targetLists[target_name]

		addrs = ip_mac[1].split("-")
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
		s.sendto(magic, (ip_mac[0], 9))
		s.close()

		return True

	except:
		return False

def GetWOLList() -> list:
	return [*targetLists]
