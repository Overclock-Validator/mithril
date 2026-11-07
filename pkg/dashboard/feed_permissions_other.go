//go:build !linux

package dashboard

import "errors"

func inspectFeedGroup(_, _ string) (int, error) {
	return 0, errors.New("dashboard feed group mode requires Linux")
}

func configureFeedGroup(_ string, _ int) error {
	return errors.New("dashboard feed group mode requires Linux")
}
