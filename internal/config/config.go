package config

import "os"

type Config struct { Addr string; DatabaseURL string }
func Load() Config { addr:=os.Getenv("APP_ADDR"); if addr=="" { addr=":8080" }; return Config{Addr:addr,DatabaseURL:os.Getenv("DATABASE_URL")} }
