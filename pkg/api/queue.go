package api

import "time"

type RegisterQueueRequest struct {
	Name     string `json:"name"`
	Priority uint   `json:"priority"`
}

type RegisterQueueResponse struct {
	QueueId string `json:"queueId"`
}

type DeleteQueueRequest struct {
	Name string `in:"path=name"`
}

type ListQueuesRequest struct {
	Page uint64 `in:"query=page"`
	Size uint64 `in:"query=size"`
}

type ListQueuesResponse struct {
	Queues []QueueInfo `json:"queues"`
}

type QueueInfo struct {
	Id           string    `json:"id"`
	RegisteredAt time.Time `json:"registeredAt"`
	PausedAt     time.Time `json:"pausedAt"`

	Name     string `json:"name"`
	Priority uint   `json:"priority"`
	Status   string `json:"status"`
}

type GetQueueRequest struct {
	Name string `in:"path=name"`
}

type GetQueueResponse QueueInfo
