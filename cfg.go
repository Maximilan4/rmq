package rmq

import (
    "time"
)

type (
    //ConnectionCfg - main connection config, is not used, because reconnect doesnt supported
    ConnectionCfg struct {
        // ReconnectTimeout - period, when process try to establish connection again.
        ReconnectTimeout time.Duration
    }

    //ConsumerConfig - main consumer config
    ConsumerConfig struct {
        // number of consuming workers
        WorkersCount int
        // run message handling in a single goroutine or in worker loop
        Synchronous bool
    }

    // PublisherConfig - main publisher config
    PublisherConfig struct {
        //MaxChannelsCount max channels count
        MaxChannelsCount int32
        //CleanUp interval - task for closing idle channels in pool
        CleanUpInterval time.Duration
        //Max idle time per one channel. Set this param smaller than CleanUpInterval
        MaxIdleTime time.Duration
    }
)
