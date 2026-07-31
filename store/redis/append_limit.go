package redis

import "fmt"

// ValidateMaxAppendBytes proves that the largest binary frame admitted by the
// HTTP body ceiling fits in one Redis bulk argument. JSON bodies split into
// smaller frames, so the binary case is the strict upper bound.
func ValidateMaxAppendBytes(maxBodyBytes, protoMaxBulkLen int64) error {
	if maxBodyBytes < 0 {
		return fmt.Errorf("max append bytes %d must be non-negative", maxBodyBytes)
	}
	if maxBodyBytes == 0 {
		return nil
	}
	prefixBytes := int64(framePrefixLn)
	if protoMaxBulkLen < prefixBytes || maxBodyBytes > protoMaxBulkLen-prefixBytes {
		return fmt.Errorf(
			"max append bytes %d plus %d-byte frame prefix exceeds Redis proto-max-bulk-len %d",
			maxBodyBytes,
			framePrefixLn,
			protoMaxBulkLen,
		)
	}
	return nil
}
