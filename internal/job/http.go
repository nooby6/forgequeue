package job
import("encoding/json";"errors";"io";"net/http";"strings")
type Handler struct{service *Service}
func NewHandler(s *Service)*Handler{return &Handler{service:s}}
func(h *Handler)Create(w http.ResponseWriter,r *http.Request){r.Body=http.MaxBytesReader(w,r.Body,1<<20);var req CreateRequest;d:=json.NewDecoder(r.Body);d.DisallowUnknownFields();if err:=d.Decode(&req);err!=nil{writeError(w,400,"invalid JSON body");return};var extra any;if err:=d.Decode(&extra);err!=io.EOF{writeError(w,400,"request body must contain one JSON value");return};if req.IdempotencyKey==""{req.IdempotencyKey=strings.TrimSpace(r.Header.Get("Idempotency-Key"))};j,err:=h.service.Create(r.Context(),req);if err!=nil{writeError(w,400,err.Error());return};writeJSON(w,201,j)}
func(h *Handler)Get(w http.ResponseWriter,r *http.Request){j,err:=h.service.Get(r.Context(),r.PathValue("id"));if errors.Is(err,ErrNotFound){writeError(w,404,"job not found");return};if err!=nil{writeError(w,500,"failed to retrieve job");return};writeJSON(w,200,j)}
func writeJSON(w http.ResponseWriter,s int,v any){w.Header().Set("Content-Type","application/json");w.WriteHeader(s);_=json.NewEncoder(w).Encode(v)}
func writeError(w http.ResponseWriter,s int,m string){writeJSON(w,s,map[string]string{"error":m})}
