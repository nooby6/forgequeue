package job
import("context";"encoding/json";"errors";"strings")
type Service struct{repo Repository}
func NewService(repo Repository)*Service{return &Service{repo:repo}}
func(s *Service)Create(ctx context.Context,req CreateRequest)(Job,error){req.QueueName=strings.TrimSpace(req.QueueName);req.JobType=strings.TrimSpace(req.JobType);if req.QueueName==""{return Job{},errors.New("queue_name is required")};if req.JobType==""{return Job{},errors.New("job_type is required")};if len(req.Payload)==0||!json.Valid(req.Payload){return Job{},errors.New("payload must be valid JSON")};if req.Priority < -100||req.Priority>100{return Job{},errors.New("priority must be between -100 and 100")};if req.MaxAttempts==0{req.MaxAttempts=3};if req.MaxAttempts<1||req.MaxAttempts>20{return Job{},errors.New("max_attempts must be between 1 and 20")};return s.repo.Create(ctx,req)}
func(s *Service)Get(ctx context.Context,id string)(Job,error){id=strings.TrimSpace(id);if id==""{return Job{},errors.New("job id is required")};return s.repo.Get(ctx,id)}
