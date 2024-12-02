package api

type RegisterQueueRequest struct {
	Name     string `json:"name"`
	Priority uint   `json:"priority"`
}

type RegisterQueueResponse struct {
	QueueId string `json:"queueId"`
}
