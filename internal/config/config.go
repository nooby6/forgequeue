package config

import ("os";"strconv";"time")

type Config struct { Addr string; DatabaseURL string; WorkerQueue string; WorkerConcurrency int; WorkerPollInterval time.Duration; WorkerLease time.Duration; WorkerRetryBackoff time.Duration }

func Load() Config {
 addr:=env("APP_ADDR",":8080");queue:=env("WORKER_QUEUE","default");concurrency:=envInt("WORKER_CONCURRENCY",4)
 poll:=envDuration("WORKER_POLL_INTERVAL",500*time.Millisecond);lease:=envDuration("WORKER_LEASE",30*time.Second);backoff:=envDuration("WORKER_RETRY_BACKOFF",1*time.Second)
 if concurrency<1{concurrency=1};if poll<=0{poll=500*time.Millisecond};if lease<=0{lease=30*time.Second};if backoff<=0{backoff=time.Second}
 return Config{Addr:addr,DatabaseURL:os.Getenv("DATABASE_URL"),WorkerQueue:queue,WorkerConcurrency:concurrency,WorkerPollInterval:poll,WorkerLease:lease,WorkerRetryBackoff:backoff}
}
func env(key,fallback string)string{if value:=os.Getenv(key);value!=""{return value};return fallback}
func envInt(key string,fallback int)int{value,err:=strconv.Atoi(os.Getenv(key));if err!=nil{return fallback};return value}
func envDuration(key string,fallback time.Duration)time.Duration{value,err:=time.ParseDuration(os.Getenv(key));if err!=nil{return fallback};return value}
