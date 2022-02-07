package rmq

import (
    "time"
)

type (
    ConnectionCfg struct {
        // ReconnectTimeout - period, when process try to establish connection again. If set 0 -> reconnect process will not be started
        ReconnectTimeout time.Duration
    }

    ConsumerConfig struct {
        // number of consuming workers
        WorkersCount int
        // run message handling in a single goroutine or in worker loop
        Synchronous bool
    }

    PublisherConfig struct {
        //MaxChannelsCount max channels count
        MaxChannelsCount int32
        //CleanUp interval - task for closing idle channels in pool
        CleanUpInterval time.Duration
        //Max idle time per one channel. Set this param smaller than CleanUpInterval
        MaxIdleTime time.Duration
    }
)
