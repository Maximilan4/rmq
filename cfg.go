package rmq

import "time"

type ConsumerConfig struct {
    Dsn string
    // number of consuming workers
    WorkersCount int
    // run message handling in a single goroutine or in worker loop
    Synchronous bool
}

type PublisherConfig struct {
    Dsn string
    //max channels count
    MaxChannelsCount int32
    //reconnect timeout
    ReconnectTimeout time.Duration
    //CleanUp interval - task for closing idle channels in pool
    CleanUpInterval time.Duration
    //Max idle time per one channel. Set this param smaller than CleanUpInterval
    MaxIdleTime time.Duration
}
