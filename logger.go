package rabbids

type Fields map[string]interface{}
type LoggerFN func(message string, fields Fields)

func NoOPLoggerFN(message string, fields Fields) {}
