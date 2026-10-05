package database

import("context";"fmt";"github.com/jackc/pgx/v5/pgxpool")
func NewPool(ctx context.Context,url string)(*pgxpool.Pool,error){if url==""{return nil,fmt.Errorf("DATABASE_URL is required")}; cfg,err:=pgxpool.ParseConfig(url);if err!=nil{return nil,fmt.Errorf("parse database config: %w",err)};pool,err:=pgxpool.NewWithConfig(ctx,cfg);if err!=nil{return nil,fmt.Errorf("create database pool: %w",err)};if err=pool.Ping(ctx);err!=nil{pool.Close();return nil,fmt.Errorf("ping database: %w",err)};return pool,nil}
