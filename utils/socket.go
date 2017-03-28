package utils

import (
	"net"
)

func GetLocalAddr(sock net.Conn) string {
	addr := sock.LocalAddr()
	return addr.String()
}

func RemoteAddr(sock net.Conn) string {
	addr := sock.RemoteAddr()
	return addr.String()
}
