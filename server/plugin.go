package server

//定义切面行为

//前端连接建立后执行
type PostFrontConnect interface {
	PostFrontConnect(*Client) error
}

//命令接受后执行
type PostCommandReceive interface {
	PostCommandReceive(*Client) error
}

//命令解析后执行
type PostCommandParse interface {
	PostCommandParse(*Client) error
}

//节点路由后执行
type PostNodeRoute interface {
	PostNodeRoute(*Client) error
}

//后端处理后执行
type PostBackendProc interface {
	PostBackendProc(*Client) error
}

//前端返回后执行
type PostFrontResponse interface {
	PostFrontResponse(*Client) error
}

type NcacheAspect interface {
	PostFrontConnect
	PostCommandReceive
	PostCommandParse
	PostNodeRoute
	PostBackendProc
	PostFrontResponse
}
