package web

type WebResult struct {
	Code int				`json:"code"`
	Msg string				`json:"msg"`
	ServerTime  int64		`json:"serverTime"`
	Data interface{}		`json:"data"`
}