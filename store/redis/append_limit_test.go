package redis

import "testing"

func TestValidateMaxAppendBytes(t *testing.T) {
	for _, test := range []struct {
		name      string
		maxBody   int64
		protoBulk int64
		wantErr   bool
	}{
		{name: "disabled", maxBody: 0, protoBulk: 1},
		{name: "fits exactly", maxBody: 100, protoBulk: 100 + framePrefixLn},
		{name: "one byte over", maxBody: 101, protoBulk: 100 + framePrefixLn, wantErr: true},
		{name: "prefix cannot fit", maxBody: 1, protoBulk: framePrefixLn - 1, wantErr: true},
		{name: "invalid minimum proto limit", maxBody: 1, protoBulk: -1 << 63, wantErr: true},
		{name: "negative", maxBody: -1, protoBulk: 1000, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateMaxAppendBytes(test.maxBody, test.protoBulk)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateMaxAppendBytes(%d, %d) error = %v, wantErr %v", test.maxBody, test.protoBulk, err, test.wantErr)
			}
		})
	}
}
