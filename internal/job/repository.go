package job
import "context"
type Repository interface{Create(context.Context,CreateRequest)(Job,error);Get(context.Context,string)(Job,error)}
