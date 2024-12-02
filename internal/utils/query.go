package utils

func ToSkipAndLimit(page uint64, size uint64) (skip uint64, limit uint64) {
	if page == 0 {
		page = 1
	}

	if size == 0 {
		size = 10
	}

	skip = (page - 1) * size
	limit = size

	return
}
