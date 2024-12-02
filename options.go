package taskq

type Options struct {
	Addr      string
	QueuePath string
	StatePath string
}

func DefaultOptions(opts *Options) *Options {
	o := &Options{
		Addr:      ":8080",
		QueuePath: "taskq/queue.db",
		StatePath: "taskq/state.db",
	}

	if len(opts.Addr) > 0 {
		o.Addr = opts.Addr
	}

	if len(opts.QueuePath) > 0 {
		o.QueuePath = opts.QueuePath
	}

	if len(opts.StatePath) > 0 {
		o.StatePath = opts.StatePath
	}

	return o
}
