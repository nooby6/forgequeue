package queue

import ("context";"errors";"time";"github.com/nooby6/forgequeue/internal/job")

var ErrNoJob=errors.New("no job available")

type Queue interface {
	ClaimNext(context.Context,string,string,time.Duration)(job.Job,error)
	Complete(context.Context,string,string)error
	Fail(context.Context,string,string)error
	Retry(context.Context,string,string,time.Duration)error
}
