package health

import("context";"encoding/json";"net/http";"github.com/jackc/pgx/v5/pgxpool")
type Handler struct{DB *pgxpool.Pool}
func(h *Handler)Live(w http.ResponseWriter,r *http.Request){writeJSON(w,http.StatusOK,map[string]string{"status":"ok"})}
func(h *Handler)Ready(w http.ResponseWriter,r *http.Request){if h.DB==nil||h.DB.Ping(context.Background())!=nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"status":"not_ready"});return};writeJSON(w,http.StatusOK,map[string]string{"status":"ready"})}
func writeJSON(w http.ResponseWriter,s int,v any){w.Header().Set("Content-Type","application/json");w.WriteHeader(s);_=json.NewEncoder(w).Encode(v)}
